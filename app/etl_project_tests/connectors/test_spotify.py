from dotenv import load_dotenv
from etl_project.connectors.spotify import SpotifyAccessTokenClient, SpotifyAPIClient
import os
import pytest


@pytest.fixture
def setup():
    load_dotenv()


def test_spotify_access_token_client(setup):
    client_id = os.environ.get("CLIENT_ID")
    client_secret = os.environ.get("CLIENT_SECRET")
    spotify_access_token_client = SpotifyAccessTokenClient(
        client_id=client_id, client_secret=client_secret
    )

    access_token = spotify_access_token_client.get_access_token()

    assert type(access_token) == str
    assert len(access_token) > 0


def test_spotify_api_client_get_playlist(setup):
    client_id = os.environ.get("CLIENT_ID")
    client_secret = os.environ.get("CLIENT_SECRET")
    spotify_access_token_client = SpotifyAccessTokenClient(
        client_id=client_id, client_secret=client_secret
    )
    spotify_api_client = SpotifyAPIClient(
        access_token_client=spotify_access_token_client)

    playlist_id = "31FWVQBp3WQydWLNhO0ACi"  # Lofi Girl's favorite playlist
    playlist_data = spotify_api_client.get_playlist_data(playlist_id)

    assert type(playlist_data) == dict
    assert "tracks" in playlist_data

    # Check if playlist contains any tracks
    assert len(playlist_data["tracks"]["items"]) > 0


def test_spotify_api_client_get_artist_from_playlist(setup):
    client_id = os.environ.get("CLIENT_ID")
    client_secret = os.environ.get("CLIENT_SECRET")
    spotify_access_token_client = SpotifyAccessTokenClient(
        client_id=client_id, client_secret=client_secret
    )
    spotify_api_client = SpotifyAPIClient(
        access_token_client=spotify_access_token_client)

    playlist_id = "31FWVQBp3WQydWLNhO0ACi"  # Lofi Girl's favorite playlist
    playlist_data = spotify_api_client.get_playlist_data(playlist_id)

    # Extract artist ID from the first track in the playlist
    first_track = playlist_data["tracks"]["items"][0]
    first_artist_id = first_track["track"]["artists"][0]["id"]

    # Fetch artist data using the extracted artist ID
    artist_data = spotify_api_client.get_artist(first_artist_id)

    assert type(artist_data) == dict
    assert artist_data["id"] == first_artist_id
    assert "name" in artist_data
