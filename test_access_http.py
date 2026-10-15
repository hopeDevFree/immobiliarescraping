import copy
import json
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlsplit

from access_http import (
    HttpScanError, diagnostic_targets, parse_search_json, probe_http,
    scan_http, summarize_response,
)
from search_config import RADIUS_URL


def api_page(ids, total, pages=1):
    return {
        "results": [{
            "realEstate": {"id": identity, "title": f"Home {identity}", "price": {"value": 800}},
            "seo": {"url": f"/annunci/{identity}/"},
        } for identity in ids],
        "totalAds": total, "maxPages": pages,
    }


def response(payload=None, text="", status=200):
    result = Mock(status_code=status, text=text)
    if payload is not None:
        result.json.return_value = payload
    else:
        result.json.side_effect = ValueError("not JSON")
    return result


def session_for(responses):
    session = Mock()
    session.__enter__ = Mock(return_value=session)
    session.__exit__ = Mock(return_value=False)
    session.get.side_effect = responses
    return session


class JsonNormalizationTests(unittest.TestCase):
    def test_historical_fixture_preserves_every_listing_and_rent(self):
        # This is a historical parser fixture, not evidence of today's live
        # exact search or a reason to adopt its previous search parameters.
        payload = json.loads(Path(__file__).with_name("immobiliare.json").read_text(encoding="utf-8"))
        original = copy.deepcopy(payload)
        parsed = parse_search_json(payload)
        self.assertEqual(parsed["total"], 14)
        self.assertEqual(parsed["maxPages"], 1)
        self.assertEqual(len(parsed["rows"]), 14)
        self.assertEqual(parsed["rows"][0], {
            "id": 132460566,
            "title": "Bilocale corso Ponticelli, Ponticelli, Napoli",
            "url": "https://www.immobiliare.it/annunci/132460566/",
            "rent_eur": 550,
        })
        self.assertEqual([row["id"] for row in parsed["rows"]], [item["realEstate"]["id"] for item in payload["results"]])
        self.assertTrue(all(set(row) == {"id", "title", "url", "rent_eur"} for row in parsed["rows"]))
        self.assertEqual(payload, original)

    def test_nextjs_dehydrated_search_is_supported(self):
        payload = api_page([1], 1)
        wrapped = {"props": {"pageProps": {"dehydratedState": {"queries": [
            {"queryKey": ["irrelevant"], "state": {"data": api_page([99], 1)}},
            {"queryKey": ["real-estate-list", "params"], "state": {"data": payload}},
        ]}}}}
        self.assertEqual(parse_search_json(wrapped), parse_search_json(payload))

    def test_explicit_empty_search_is_distinct_from_missing_or_challenge(self):
        self.assertEqual(parse_search_json(api_page([], 0, 0)), {"rows": [], "total": 0, "maxPages": 0})
        self.assertIsNone(parse_search_json({"results": [], "totalAds": 27}))
        self.assertIsNone(parse_search_json({"url": "https://geo.captcha-delivery.com/captcha/?cid=secret-value"}))
        malformed = api_page([1], 1)
        malformed["results"][0]["realEstate"]["id"] = True
        self.assertIsNone(parse_search_json(malformed))


class HttpScanTests(unittest.TestCase):
    def test_api_reads_all_pages_deduplicates_and_only_changes_page_parameter(self):
        session = session_for([
            response(api_page([1, 2], 3, 2)),
            response(api_page([2, 3], 3, 2)),
        ])
        url = RADIUS_URL.replace("/search-list/", "/api-next/search-list/listings/", 1) + "&path=%2Fsearch-list%2F"
        with patch("access_http._new_session", return_value=session) as factory:
            rows = scan_http("chrome", url)
        factory.assert_called_once_with("chrome")
        self.assertEqual([row["id"] for row in rows], [1, 2, 3])
        self.assertEqual([call.args[0] for call in session.get.call_args_list], [url, url.replace("&pag=1", "&pag=2")])
        self.assertTrue(all(call.kwargs == {"timeout": 15, "allow_redirects": False} for call in session.get.call_args_list))
        session.__exit__.assert_called_once()

    def test_html_pagination_uses_same_query_and_preserves_expensive_listing(self):
        def html(identity, link=""):
            return f'''<html><title>Case in affitto</title><p>2 risultati</p>
            <ul data-cy="listing-search-results"><li id="{identity}">
            <a href="/annunci/{identity}/" title="Home {identity}">Home</a>
            <span>\u20ac 1.600/mese</span></li></ul>{link}</html>'''
        session = session_for([
            response(text=html(1, '<a href="?pag=2">2</a>')),
            response(text=html(2)),
        ])
        with patch("access_http._new_session", return_value=session):
            rows = scan_http("safari", RADIUS_URL)
        self.assertEqual([row["rent_eur"] for row in rows], [1600, 1600])
        self.assertEqual(session.get.call_args_list[0].args[0], RADIUS_URL)
        self.assertEqual(session.get.call_args_list[1].args[0], RADIUS_URL.replace("&pag=1", "&pag=2"))

    def test_missing_listing_or_changing_total_never_returns_partial_success(self):
        for pages, category in (
            ([api_page([1], 3, 2), api_page([2], 3, 2)], "incomplete_results"),
            ([api_page([1], 2, 2), api_page([2], 3, 2)], "total_changed"),
        ):
            with self.subTest(category=category):
                session = session_for([response(page) for page in pages])
                with patch("access_http._new_session", return_value=session):
                    with self.assertRaises(HttpScanError) as raised:
                        scan_http("chrome", RADIUS_URL)
                self.assertEqual(raised.exception.category, category)

    def test_page_limit_stops_after_first_response(self):
        session = session_for([response(api_page([1], 200, 11))])
        with patch("access_http._new_session", return_value=session):
            with self.assertRaisesRegex(HttpScanError, "category=page_limit"):
                scan_http("chrome", RADIUS_URL, max_pages=10)
        session.get.assert_called_once()

    def test_html_and_json_explicit_empty_searches_are_valid(self):
        for empty in (response(api_page([], 0, 0)), response(text="<p>0 risultati</p>")):
            session = session_for([empty])
            with patch("access_http._new_session", return_value=session):
                self.assertEqual(scan_http("chrome", RADIUS_URL), [])

    def test_challenges_are_sanitized_even_with_http_200(self):
        secret = "DO-NOT-LOG-THIS-SESSION"
        challenge = {"url": f"https://geo.captcha-delivery.com/captcha/?cid={secret}"}
        for blocked in (
            response(challenge),
            response(text=f"<pre>{json.dumps(challenge)}</pre>"),
            response(text=f"<p>Please enable JS and disable any ad blocker</p>{secret}", status=403),
        ):
            with self.subTest(blocked=blocked):
                session = session_for([blocked])
                with patch("access_http._new_session", return_value=session):
                    with self.assertRaises(HttpScanError) as raised:
                        scan_http("chrome", RADIUS_URL)
                self.assertEqual(raised.exception.category, "captcha")
                self.assertNotIn(secret, str(raised.exception))
                self.assertIsNone(raised.exception.__cause__)
                self.assertNotIn(secret, json.dumps(summarize_response(blocked)))
                session.get.assert_called_once()

    def test_transport_and_http_failures_omit_exception_and_response_contents(self):
        secret = "NEVER-SERIALIZE-THIS"
        transport_error = RuntimeError(secret)
        transport_error.code = 7
        for item, category in (
            (transport_error, "transport"),
            (response({"message": secret}, status=403), "blocked"),
            (response({"message": secret}, status=503), "http_status"),
        ):
            session = session_for([item])
            with patch("access_http._new_session", return_value=session):
                with self.assertRaises(HttpScanError) as raised:
                    scan_http("chrome", RADIUS_URL)
            self.assertEqual(raised.exception.category, category)
            self.assertNotIn(secret, str(raised.exception))
            if category == "transport":
                self.assertEqual(raised.exception.transport_error_code, 7)


class DiagnosticMatrixTests(unittest.TestCase):
    def test_matrix_is_exactly_six_requests_with_one_explicit_technical_parameter(self):
        sessions = [session_for([response(api_page([1], 1))] * 4), session_for([response(api_page([1], 1))] * 2)]
        with patch("access_http._new_session", side_effect=sessions):
            result = probe_http()
        self.assertEqual(result["requests"], 6)
        self.assertEqual([entry["profile"] for entry in result["results"]], ["chrome"] * 4 + ["safari"] * 2)
        expected_query = parse_qs(urlsplit(RADIUS_URL).query)
        technical = 0
        for _, url in diagnostic_targets():
            query = parse_qs(urlsplit(url).query)
            if "path" in query:
                self.assertEqual(query.pop("path"), ["/search-list/"])
                technical += 1
            self.assertEqual(query, expected_query)
            self.assertTrue(urlsplit(url).query.startswith(urlsplit(RADIUS_URL).query))
        self.assertEqual(technical, 1)
        self.assertEqual(sum(not entry["query_unchanged"] for entry in result["results"]), 1)
        self.assertTrue(all(entry["filters_unchanged"] for entry in result["results"]))


if __name__ == "__main__":
    unittest.main()
