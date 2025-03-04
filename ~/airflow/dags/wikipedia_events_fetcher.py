from bs4 import BeautifulSoup
from os.path import expanduser, join
import requests


class WikipediaEventsFetcher:
    def __init__(self, output_file: str = None):
        """Initialize the fetcher with default configuration."""
        self.base_url = "https://en.wikipedia.org/wiki/Wikipedia:On_this_day/Today"
        home = expanduser("~")
        self.output_file = output_file or join(home, "Desktop/test_2.txt")

    def get_ul(self, url: str):
        """Fetches the <ul> element from a given URL."""
        response = requests.get(url)
        soup = BeautifulSoup(response.content, "html.parser")
        return soup.find("ul")

    def write_file(self, text: str):
        """Writes a line of text to the output file."""
        with open(self.output_file, "a") as file:
            file.write(text + "\n")

    def get_today_events(self):
        """Fetches today's events from Wikipedia and writes them to a file."""
        ul_soup = self.get_ul(self.base_url)
        if ul_soup:
            for li in ul_soup.find_all("li"):
                self.write_file(li.text)
