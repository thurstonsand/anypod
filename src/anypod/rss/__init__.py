# Pre-import lxml.etree before feedgen to work around Python 3.14 issue.
# feedgen does `import lxml` then accesses `lxml.etree` without explicit import.
# In Python 3.14, submodules aren't auto-populated on parent module import.
# See: https://stackoverflow.com/q/41066480
__import__("lxml.etree")

from .rss_feed import RSSFeedGenerator  # noqa: E402

__all__ = ["RSSFeedGenerator"]
