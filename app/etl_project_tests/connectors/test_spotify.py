import os
from dotenv import load_dotenv
import pytest
from etl_project.connectors.spotify import SpotifyAccessTokenClient, SpotifyAPIClient


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
        access_token_client=spotify_access_token_client
    )

    playlist_id = "31FWVQBp3WQydWLNhO0ACi"  # Lofi Girl's favorite playlist

    # Fetch playlist data with pagination considered
    playlist_data, all_tracks = spotify_api_client.get_playlist_data(
        playlist_id)

    # Ensure that the first element is a dictionary (playlist metadata)
    assert type(playlist_data) == dict
    assert "tracks" in playlist_data

    # Ensure that the second element is a list (all tracks)
    assert type(all_tracks) == list
    assert len(all_tracks) > 0


def test_spotify_api_client_get_artist_from_playlist(setup):
    client_id = os.environ.get("CLIENT_ID")
    client_secret = os.environ.get("CLIENT_SECRET")
    spotify_access_token_client = SpotifyAccessTokenClient(
        client_id=client_id, client_secret=client_secret
    )
    spotify_api_client = SpotifyAPIClient(
        access_token_client=spotify_access_token_client
    )

    playlist_id = "31FWVQBp3WQydWLNhO0ACi"  # Lofi Girl's favorite playlist

    # Fetch playlist data considering pagination
    playlist_data, all_tracks = spotify_api_client.get_playlist_data(
        playlist_id)

    # Extract artist ID from the first track in the playlist
    # Access tracks from `all_tracks` (second element of the tuple)
    first_track = all_tracks[0]
    first_artist_id = first_track["track"]["artists"][0]["id"]

    # Fetch artist data using the extracted artist ID
    artist_data = spotify_api_client.get_artist(first_artist_id)

    assert type(artist_data) == dict
    assert artist_data["id"] == first_artist_id
    assert "name" in artist_data
