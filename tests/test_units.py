"""Parsing, rendering and redaction."""

from __future__ import annotations

import math

from conftest import fixture

from cephmcp import promtext, render
from cephmcp.metrics import addr_host, metrics_url
from cephmcp.redact import REDACTED, redact_obj, redact_text

AES128_KEY = "AQ" + "B" * 36 + "=="
AES256_KEY = "Ag" + "C" * 57 + "="


class TestPromtext:
    def test_types_help_and_labels(self) -> None:
        metrics = promtext.parse(
            "# HELP ceph_x Something useful\n"
            "# TYPE ceph_x counter\n"
            'ceph_x{ceph_daemon="osd.0",pool="a b"} 12\n'
            'ceph_x{ceph_daemon="osd.1"} 3.5e2 1700000000\n'
            "ceph_plain 7\n"
        )
        x = metrics["ceph_x"]
        assert (x.type, x.help) == ("counter", "Something useful")
        assert [s.value for s in x.samples] == [12.0, 350.0]
        assert x.samples[0].labels == (("ceph_daemon", "osd.0"), ("pool", "a b"))
        assert x.samples[0].label("pool") == "a b"
        assert x.samples[0].label("missing", "-") == "-"
        assert metrics["ceph_plain"].type == "untyped"

    def test_escaped_label_values(self) -> None:
        metrics = promtext.parse('m{a="x\\"y\\\\z,}{",b="line\\nbreak"} 1\n')
        assert metrics["m"].samples[0].labels == (("a", 'x"y\\z,}{'), ("b", "line\nbreak"))

    def test_special_values_and_junk(self) -> None:
        metrics = promtext.parse('a NaN\nb +Inf\nc -Inf\nbroken{x="1"}\n\n# just a comment\nd{ 1\n')
        assert math.isnan(metrics["a"].samples[0].value)
        assert metrics["b"].samples[0].value == math.inf
        assert metrics["c"].samples[0].value == -math.inf
        assert "broken" not in metrics and "d" not in metrics

    def test_histogram_series_inherit_family_type(self) -> None:
        metrics = promtext.parse('# TYPE h histogram\nh_bucket{le="1"} 2\nh_sum 3\nh_count 2\n')
        assert {m.type for m in metrics.values()} == {"histogram"}

    def test_real_exporter_output(self) -> None:
        mgr = promtext.parse(fixture("mgr_metrics.txt"))
        assert mgr["ceph_health_status"].samples[0].value == 1.0
        assert len(mgr["ceph_osd_up"].samples) == 3
        assert mgr["ceph_osd_metadata"].samples[0].label("hostname") == "cephmcp-adm"
        exporter = promtext.parse(fixture("exporter_metrics.txt"))
        assert len(exporter["ceph_osd_op_w_latency_sum"].samples) == 3
        # ':' is legal in metric names and ceph-exporter uses it
        assert "ceph_prioritycache:full_pri0_bytes" in exporter


class TestRender:
    def test_fmt_bytes(self) -> None:
        assert render.fmt_bytes(0) == "0 B"
        assert render.fmt_bytes(1023) == "1023 B"
        assert render.fmt_bytes(1536) == "1.5 KiB"
        assert render.fmt_bytes(32199671808) == "30 GiB"
        assert render.fmt_bytes(None) is None
        assert render.fmt_rate(1048576) == "1 MiB/s"

    def test_table_drops_empty_columns_and_rounds(self) -> None:
        table = render.table([{"a": 1.23456, "b": None}, {"a": 2.0, "b": None, "c": "x"}])
        assert table == {"columns": ["a", "c"], "rows": [[1.235, None], [2, "x"]]}

    def test_limited_reports_what_was_left_out(self) -> None:
        out = render.limited([{"n": i} for i in range(5)], 2, what="OSDs")
        assert out["rows"] == [[0], [1]]
        assert out["count"] == 5
        assert "2 of 5 OSDs" in out["truncated"]
        assert "truncated" not in render.limited([{"n": 1}], 2)

    def test_compact_keeps_zero_and_false(self) -> None:
        assert render.compact({"a": None, "b": [], "c": {"d": {}}, "e": 0, "f": False}) == {
            "e": 0,
            "f": False,
        }

    def test_num_drops_nan(self) -> None:
        assert render.num(float("nan")) is None
        assert render.num(float("inf")) is None
        assert render.num(3.0) == 3 and isinstance(render.num(3.0), int)
        assert render.num(True) is True

    def test_clip(self) -> None:
        assert render.clip("short", 100) == "short"
        clipped = render.clip("x" * 500, 100)
        assert clipped.startswith("x" * 100) and "100 of 500" in clipped


class TestRedact:
    def test_keys_in_text(self) -> None:
        text = redact_text(f"[client.admin]\n\tkey = {AES128_KEY}\nval={AES256_KEY}: bad\n")
        assert AES128_KEY not in text and AES256_KEY not in text
        assert text.count(REDACTED) == 2
        assert "[client.admin]" in text

    def test_leaves_ordinary_text_alone(self) -> None:
        text = "pool 'rbd' created; AQshort== is not a key; osd.1 up"
        assert redact_text(text) == text

    def test_private_key_blocks(self) -> None:
        pem = "-----BEGIN RSA PRIVATE KEY-----\nMIIabc\n-----END RSA PRIVATE KEY-----"
        assert redact_text(f"cert follows\n{pem}\nend") == f"cert follows\n{REDACTED}\nend"

    def test_json_fields_in_text(self) -> None:
        text = redact_text('{"user":"u","secret_key":"s3cr3t","password":"hunter2","pg_num":32}')
        assert "s3cr3t" not in text and "hunter2" not in text
        assert '"user":"u"' in text and '"pg_num":32' in text

    def test_option_tables(self) -> None:
        table = (
            "WHO     LEVEL     OPTION                              VALUE\n"
            "mgr     advanced  mgr/dashboard/GRAFANA_API_PASSWORD  hunter2\n"
            "osd     advanced  osd_max_backfills                   3\n"
            "rgw_ldap_secret = /etc/ldap.secret\n"
        )
        text = redact_text(table)
        assert "hunter2" not in text and "/etc/ldap.secret" not in text
        assert "osd_max_backfills                   3" in text
        # prose that merely mentions a password is not an option/value pair
        prose = "set the password for the user; token bucket 5; mon_allow_pool_delete true"
        assert redact_text(prose) == prose

    def test_objects(self) -> None:
        out = redact_obj(
            [
                {"entity": "client.admin", "key": AES128_KEY, "caps": {"mon": "allow *"}},
                {"name": "rgw_keystone_admin_password", "value": "hunter2"},
                {"name": "osd_max_backfills", "value": "3"},
                {"access_key": "AKIA", "max_key": 5, "token": ""},
            ]
        )
        assert out[0] == {"entity": "client.admin", "key": REDACTED, "caps": {"mon": "allow *"}}
        assert out[1]["value"] == REDACTED
        assert out[2]["value"] == "3"
        # numbers and empty strings are not secrets, whatever the field is called
        assert out[3] == {"access_key": REDACTED, "max_key": 5, "token": ""}
        row = {"who": "client.rgw", "option": "rgw_keystone_admin_password", "value": "s3cr3t"}
        assert redact_obj(row)["value"] == REDACTED

    def test_tables(self) -> None:
        table = {
            "columns": ["option", "value"],
            "rows": [["rgw_keystone_admin_password", "s3cr3t"], ["osd_max_backfills", "3"]],
            "count": 2,
        }
        assert redact_obj({"result": table})["result"] == {
            "columns": ["option", "value"],
            "rows": [["rgw_keystone_admin_password", REDACTED], ["osd_max_backfills", "3"]],
            "count": 2,
        }
        keys = {"columns": ["entity", "key"], "rows": [["client.a", AES128_KEY]]}
        assert redact_obj(keys)["rows"] == [["client.a", REDACTED]]


class TestAddresses:
    def test_metrics_url(self) -> None:
        assert metrics_url("http://h:9283/") == "http://h:9283/metrics"
        assert metrics_url("h:9926") == "http://h:9926/metrics"
        assert metrics_url("https://h:9283/metrics") == "https://h:9283/metrics"

    def test_addr_host(self) -> None:
        assert addr_host("10.0.0.1:6800/123") == "10.0.0.1"
        assert addr_host("[fd42::1]:6789/0") == "fd42::1"
        assert addr_host("") is None and addr_host("not-an-ip:1") is None
