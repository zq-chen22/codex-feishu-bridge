from codex_feishu_bridge.privacy import log_ref, redact_log


def test_log_redaction_removes_common_identifiers_and_credentials() -> None:
    raw = (
        "app_secret=example-secret "
        "Bearer example-token "
        "/home/private-user/project "
        "ou_abcdefghijk user@example.com "
        "123e4567-e89b-12d3-a456-426614174000"
    )

    redacted = redact_log(raw)

    for private in (
        "example-secret",
        "example-token",
        "private-user",
        "ou_abcdefghijk",
        "user@example.com",
        "123e4567-e89b-12d3-a456-426614174000",
    ):
        assert private not in redacted


def test_log_reference_is_stable_and_non_revealing() -> None:
    assert log_ref("private-id") == log_ref("private-id")
    assert log_ref("private-id") != log_ref("other-id")
    assert "private-id" not in log_ref("private-id")
