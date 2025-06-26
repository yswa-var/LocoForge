"""Noop license middleware"""


async def get_license_status() -> bool:
    """Always return true"""
    return True


def plus_features_enabled() -> bool:
    """Always return false"""
    return False


async def check_license_periodically(_: int = 60):
    """
    Periodically re-verify the license.
    If the license ever fails, you could decide to log,
    raise an exception, or attempt a graceful shutdown.
    """
    raise NotImplementedError(
        "This is a noop license middleware. No license check is performed."
    )
