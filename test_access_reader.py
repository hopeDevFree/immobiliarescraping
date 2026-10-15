import json
import unittest
from unittest.mock import Mock, patch

from access_http import HttpScanError, diagnostic_targets
from access_reader import READER_PREFIX, scan_reader


def response(ids=(), total=0, pages=1, payload=None, status=200):
    if payload is None:
        payload = {
            "results": [{
                "realEstate": {"id": identity, "title": f"Home {identity}", "price": {"value": 900}},
                "seo": {"url": f"/annunci/{identity}/"},
            } for identity in ids],
            "totalAds": total, "maxPages": pages,
        }
    return Mock(status_code=status, text="Title:\nURL Source: public-api\n\nMarkdown Content:\n" + json.dumps(payload))


class ReaderTests(unittest.TestCase):
    def test_all_pages_use_no_cache_header_and_unchanged_filters(self):
        first = diagnostic_targets()[2][1]
        with patch("curl_cffi.requests.get", side_effect=[response([1, 2], 3, 2), response([2, 3], 3, 2)]) as get:
            rows = scan_reader()
        self.assertEqual([row["id"] for row in rows], [1, 2, 3])
        self.assertEqual([call.args[0] for call in get.call_args_list], [
            READER_PREFIX + first, READER_PREFIX + first.replace("&pag=1", "&pag=2"),
        ])
        for call in get.call_args_list:
            self.assertEqual(call.kwargs, {"timeout": 30, "headers": {"X-No-Cache": "true"}, "allow_redirects": False})

    def test_captcha_and_invalid_body_never_become_empty_search(self):
        secret = "DO-NOT-LOG-CHALLENGE"
        challenge = response(payload={"url": f"https://geo.captcha-delivery.com/captcha/?cid={secret}"})
        invalid = Mock(status_code=200, text="Markdown Content:\nPlease enable JS " + secret)
        for result, category in ((challenge, "reader_captcha"), (invalid, "reader_invalid_json")):
            with self.subTest(category=category), patch("curl_cffi.requests.get", return_value=result) as get:
                with self.assertRaises(HttpScanError) as raised:
                    scan_reader()
                self.assertEqual(raised.exception.category, category)
                self.assertNotIn(secret, str(raised.exception))
                get.assert_called_once()
        with patch("curl_cffi.requests.get", return_value=response([], 0, 0)):
            self.assertEqual(scan_reader(), [])

    def test_incomplete_changed_or_excessive_pages_fail_without_partial_results(self):
        for replies, category in (
            ([response([1], 3, 2), response([2], 3, 2)], "reader_incomplete_results"),
            ([response([1], 2, 2), response([2], 3, 2)], "reader_total_changed"),
            ([response([1], 50, 11)], "reader_page_limit"),
        ):
            with self.subTest(category=category), patch("curl_cffi.requests.get", side_effect=replies):
                with self.assertRaises(HttpScanError) as raised:
                    scan_reader()
                self.assertEqual(raised.exception.category, category)

    def test_transport_and_http_errors_are_sanitized(self):
        secret = "PRIVATE-RESPONSE-CONTENTS"
        error = RuntimeError(secret)
        error.code = 7
        with patch("curl_cffi.requests.get", side_effect=error):
            with self.assertRaises(HttpScanError) as raised:
                scan_reader()
        self.assertEqual(raised.exception.category, "reader_transport")
        self.assertEqual(raised.exception.transport_error_code, 7)
        self.assertNotIn(secret, str(raised.exception))
        with patch("curl_cffi.requests.get", return_value=Mock(status_code=429, text=secret)):
            with self.assertRaisesRegex(HttpScanError, "reader_http_status.*429") as raised:
                scan_reader()
        self.assertNotIn(secret, str(raised.exception))


if __name__ == "__main__":
    unittest.main()
