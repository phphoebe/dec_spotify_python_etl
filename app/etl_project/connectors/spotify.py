import requests
import base64


class SpotifyAccessTokenClient:
    def __init__(self, client_id: str, client_secret: str) -> None:
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = None

    def get_access_token(self) -> str:
        """
        Fetch the Spotify API access token using client credentials.
        """
        auth_str = f"{self.client_id}:{self.client_secret}"
        auth_bytes = auth_str.encode('utf-8')
        auth_base64 = base64.b64encode(auth_bytes).decode('utf-8')

        url = "https://accounts.spotify.com/api/token"
        headers = {
            'Authorization': f"Basic {auth_base64}",
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        data = {'grant_type': 'client_credentials'}

        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            self.access_token = response.json()['access_token']
            return self.access_token
        else:
            raise Exception(
                f"Failed to get access token: {response.status_code} - {response.text}")


class SpotifyAPIClient:
    def __init__(self, access_token_client: SpotifyAccessTokenClient):
        """
        Initialize the SpotifyAPIClient with an instance of SpotifyAccessTokenClient.

        Args:
            access_token_client (SpotifyAccessTokenClient): An instance to handle token generation.
        """
        self.access_token_client = access_token_client
        self.access_token = self.access_token_client.get_access_token()

    def get_playlist_data(self, playlist_id: str) -> dict:
        """
        Fetch playlist data from Spotify API.

        Args:
            playlist_id (str): The Spotify playlist ID.

        Returns:
            dict: The playlist data from the API.
        """
        playlist_url = f"https://api.spotify.com/v1/playlists/{playlist_id}"
        headers = {
            'Authorization': f"Bearer {self.access_token}"
        }

        response = requests.get(playlist_url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                f"Error fetching playlist data: {response.status_code}")

    def get_artist(self, artist_id: str) -> dict:
        """
        Fetch artist data from Spotify API.

        Args:
            artist_id (str): The Spotify artist ID.

        Returns:
            dict: The artist data from the API.
        """
        artist_url = f"https://api.spotify.com/v1/artists/{artist_id}"
        headers = {
            'Authorization': f"Bearer {self.access_token}"
        }

        response = requests.get(artist_url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                f"Error fetching artist data: {response.status_code}")
