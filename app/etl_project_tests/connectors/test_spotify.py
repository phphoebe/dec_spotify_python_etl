import os
from dotenv import load_dotenv
import pytest
from etl_project.connectors.spotify import SpotifyAccessTokenClient, SpotifyAPIClient


@pytest.fixture
def setup():
    """
    Fixture to load environment variables from the .env file before each test.
    """
    load_dotenv()


def test_spotify_access_token_client(setup):
    """
    Test the functionality of the SpotifyAccessTokenClient to ensure it retrieves a valid access token.

    Asserts:
        - The access token is a non-empty string.
    """
    client_id = os.environ.get("CLIENT_ID")
    client_secret = os.environ.get("CLIENT_SECRET")
    spotify_access_token_client = SpotifyAccessTokenClient(
        client_id=client_id, client_secret=client_secret
    )

    # Get access token
    access_token = spotify_access_token_client.get_access_token()

    # Assert token is a valid non-empty string
    assert type(access_token) == str
    assert len(access_token) > 0


def test_spotify_api_client_get_playlist(setup):
    """
    Test the functionality of SpotifyAPIClient to fetch playlist data along with all tracks.

    Asserts:
        - The first returned value is the playlist metadata (dictionary).
        - The second returned value is a list of all tracks (list).
    """
    client_id = os.environ.get("CLIENT_ID")
    client_secret = os.environ.get("CLIENT_SECRET")
    spotify_access_token_client = SpotifyAccessTokenClient(
        client_id=client_id, client_secret=client_secret
    )
    spotify_api_client = SpotifyAPIClient(
        access_token_client=spotify_access_token_client
    )

    playlist_id = "31FWVQBp3WQydWLNhO0ACi"  # Lofi Girl's favorite playlist

    # Fetch playlist metadata and all tracks
    playlist_data, all_tracks = spotify_api_client.get_playlist_data(
        playlist_id)

    # Assert the playlist data is a dictionary
    assert type(playlist_data) == dict
    assert "tracks" in playlist_data

    # Assert the all_tracks list contains track data
    assert type(all_tracks) == list
    assert len(all_tracks) > 0


def test_spotify_api_client_get_artist_from_playlist(setup):
    """
    Test SpotifyAPIClient's ability to fetch artist data by extracting the first artist ID from a playlist.

    Asserts:
        - Artist data is a dictionary.
        - The artist ID matches the extracted artist ID.
        - The artist's name exists in the response.
    """
    client_id = os.environ.get("CLIENT_ID")
    client_secret = os.environ.get("CLIENT_SECRET")
    spotify_access_token_client = SpotifyAccessTokenClient(
        client_id=client_id, client_secret=client_secret
    )
    spotify_api_client = SpotifyAPIClient(
        access_token_client=spotify_access_token_client
    )

    playlist_id = "31FWVQBp3WQydWLNhO0ACi"  # Lofi Girl's favorite playlist

    # Fetch playlist metadata and all tracks
    playlist_data, all_tracks = spotify_api_client.get_playlist_data(
        playlist_id)

    # Extract artist ID from the first track
    first_track = all_tracks[0]
    first_artist_id = first_track["track"]["artists"][0]["id"]

    # Fetch artist data using the extracted artist ID
    artist_data = spotify_api_client.get_artist(first_artist_id)

    # Assert that the artist data is a dictionary and contains the correct artist ID and name
    assert type(artist_data) == dict
    assert artist_data["id"] == first_artist_id
    assert "name" in artist_data
