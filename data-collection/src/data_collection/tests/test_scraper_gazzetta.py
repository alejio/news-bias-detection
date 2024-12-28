import pytest
from unittest.mock import Mock
import requests
from data_collection.scraper_gazzetta import GazzettaScraper

@pytest.fixture
def mock_response():
    return """
    <html>
        <div class="blogger-card">
            <a href="/journalist1">
                <span class="blogger-name">John Doe</span>
            </a>
        </div>
        <div class="article-card">
            <a href="/article1">
                <h2 class="article-title">Test Article</h2>
            </a>
            <span class="article-date">2024-01-01</span>
        </div>
    </html>
    """

@pytest.fixture
def scraper():
    return GazzettaScraper()

class TestGazzettaScraper:
    def test_get_journalists(self, scraper, mocker, mock_response):
        """Test journalist extraction from HTML"""
        mock_get = mocker.patch('requests.get')
        mock_get.return_value = Mock(text=mock_response)

        journalists = scraper.get_journalists()

        assert len(journalists) == 1
        assert journalists[0] == {
            'name': 'John Doe',
            'profile_url': 'https://www.gazzetta.gr/journalist1'
        }