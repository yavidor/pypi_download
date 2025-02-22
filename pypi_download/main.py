from __future__ import annotations
import argparse
import dataclasses
import functools
import itertools
import logging
import pathlib
import re
import threading
import requests
import parsel
import urllib.parse
import contextvars
import pkginfo
import tqdm

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
download_dir: contextvars.ContextVar[pathlib.Path] = contextvars.ContextVar("dowload_dir")
repository: contextvars.ContextVar[Repository] = contextvars.ContextVar("repository")

@dataclasses.dataclass(frozen=True)
class Repository:
    base_url: str

    def _get(self, endpoint: str) -> requests.Response:
        response = requests.get(self.base_url + endpoint)
        response.raise_for_status()
        return response
    
    def packages(self):
        html = self._get("/simple").text
        selector = parsel.Selector(html)
        for anchor in selector.css("a"):
            yield Package(anchor.css("::text").get())
    
    def distributions(self, package: 'Package' | str):
        if isinstance(package, str):
            package = Package(package)
        html = self._get("/simple/" + urllib.parse.quote(package.name)).text
        selector = parsel.Selector(html)
        distributions = []
        for anchor in selector.css("a"):
            full_name = anchor.css("::text").get()
            url = anchor.attrib["href"]
            distributions.insert(0, Distribution(package, full_name=full_name, repository=self, url=url))
        return distributions


@dataclasses.dataclass(frozen=True)
class Package:
    name: str

@dataclasses.dataclass(frozen=True)
class Distribution:
    package: Package
    full_name: str
    repository: Repository
    url: str

    @property
    def dest(self) -> pathlib.Path:
        return download_dir.get() / self.package.name / self.full_name

    @property
    def dependencies(self) -> list[Package]:
        path = str(self.dest)
        metadata = pkginfo.get_metadata(path)
        if not metadata or not metadata.requires_dist:
            return []
        
        dependencies = []
        name_pattern = re.compile(r"([a-zA-Z0-9_]+).*")
        for requirement in metadata.requires_dist:
            match = name_pattern.match(requirement)
            if not match:
                raise ValueError(f"Couldnt parse requirement {requirement!r}")
            dependencies.insert(0, Package(match.group(1)))
        return dependencies

    def download(self):
        dest = self.dest
        dest.parent.mkdir(parents=True, exist_ok=True)

        if dest.exists():
            return
        response = requests.get(self.url)
        response.raise_for_status()
        with open(dest, "wb") as fp:
            fp.write(response.content)


class RecursiveDownloadManager:
    def __init__(self, package: Package):
        self.package = package
        self.visited: set[Distribution] = set()
        self.load_bar = tqdm.tqdm()
        self.threads: list[threading.Thread] = []
    
        
    def run(self):
        distributions  = list(repository.get().distributions(self.package))
        self.load_bar.total = self.load_bar.total or 0 + len(distributions)
        for distribution in distributions:
            self.recurse(distribution)

        for dependency in distribution.dependencies:
            for dependency_distribution in repository.get().distributions(dependency):
                self.recurse(dependency_distribution)


    def recurse(self, distribution: Distribution):
        if distribution in self.visited:
            return

        self.visited.add(distribution)
        self.load_bar.set_description(f"Downloading {distribution.full_name}")
        distribution.download()
        self.load_bar.update()
        self.load_bar.total += len(distribution.dependencies)
        self.load_bar.refresh()

        dependencies = distribution.dependencies
        for dependency in dependencies:
            for dependency_distribution in repository.get().distributions(dependency):
                self.recurse(dependency_distribution)

    
    def wait(self):
        for thread in self.threads:
            thread.join()

def recursivly_download(
        package: Package,
):
    download_dir.get().mkdir(exist_ok=True)
    manager = RecursiveDownloadManager(package)
    manager.run()
    manager.wait()
    

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("-p", "--package", required=True, type=str)
    parser.add_argument("-d", "--download_dir", type=str, default="./downloads")
    parser.add_argument("-r", "--repository", type=str, default="https://pypi.org")
    args = parser.parse_args()

    package = Package(args.package)
    download_dir.set(pathlib.Path(args.download_dir))
    repository.set(Repository(args.repository))
    recursivly_download(
        package=package,
    )

