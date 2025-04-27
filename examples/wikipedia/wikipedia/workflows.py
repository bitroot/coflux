import requests
import datetime as dt
import typing as t
import nltk
import collections
import coflux as cf

from bs4 import BeautifulSoup

# TODO
USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64; rv:94.0) Gecko/20100101 Firefox/94.0"


def _wiki_get(url):
    r = requests.get(url, headers={"User-Agent": USER_AGENT})
    r.raise_for_status()
    return r


@cf.task(memo=True)
def top_pages(date):
    cf.log_info("Fetching top pages for {date}...", date=date)
    date = dt.date.fromisoformat(date)
    url = f"https://wikimedia.org/api/rest_v1/metrics/pageviews/top/en.wikipedia/all-access/{date.year}/{date.month:02d}/{date.day:02d}"
    return _wiki_get(url).json()["items"][0]["articles"]


@cf.task(memo=True)
def fetch_article_metadata(name: str):
    cf.log_info("Fetching metadata for page '{name}'...", name=name)
    return _wiki_get(f"https://en.wikipedia.org/api/rest_v1/page/title/{name}").json()[
        "items"
    ][0]


@cf.task(cache=True)
def fetch_article_content(name: str, revision: int):
    cf.log_info("Fetching content for page '{name}'...", name=name, revision=revision)
    return _wiki_get(
        f"https://en.wikipedia.org/api/rest_v1/page/html/{name}/{revision}"
    ).text


@cf.task(memo=True)
def convert_to_text(html: str):
    soup = BeautifulSoup(html, "html.parser")
    return soup.get_text()


@cf.task(wait=True)
def tokenise(text_: cf.Execution[str]):
    return nltk.word_tokenize(text_.result())


@cf.task(wait=True)
def count_tokens(tokens_: cf.Execution[t.List[str]]):
    return collections.Counter(tokens_.result()).most_common(100)


@cf.task()
def process_article(name: str):
    metadata = fetch_article_metadata(name)
    content = fetch_article_content(name, metadata["rev"])
    return count_tokens.submit(tokenise.submit(convert_to_text.submit(content)))


def _iso_yesterday():
    today = dt.datetime.now(dt.timezone.utc).date()
    yesterday = today - dt.timedelta(days=1)
    return yesterday.isoformat()


def _filter_articles(article_names: list[str]) -> list[str]:
    return [
        a for a in article_names if not a.startswith("Special:") and a != "Main_Page"
    ]


@cf.workflow()
def wikipedia_workflow(date: str | None = None, n: int = 3):
    date = date or _iso_yesterday()
    cf.log_info("Starting workflow...", date=date)
    article_names = _filter_articles([a["article"] for a in top_pages(date)])
    cf.log_info(
        "Found {count} articles. Using top {n}...",
        count=len(article_names),
        n=n,
    )
    return [process_article.submit(a) for a in article_names[:n]]
