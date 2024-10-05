import requests
import base64


class SpotifyAccessTokenClient:
    """
    A client for obtaining an access token from Spotify's API using client credentials.

    Attributes:
        client_id (str): The Spotify Client ID.
        client_secret (str): The Spotify Client Secret.
        access_token (str): The access token obtained from Spotify API.
    """

    def __init__(self, client_id: str, client_secret: str) -> None:
        """
        Initialize the SpotifyAccessTokenClient with client credentials.

        Args:
            client_id (str): The Spotify Client ID.
            client_secret (str): The Spotify Client Secret.
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = None

    def get_access_token(self) -> str:
        """
        Fetch the Spotify API access token using client credentials.

        Sends a request to Spotify's token endpoint to retrieve an access token 
        required for authenticating subsequent API calls.

        Returns:
            str: The access token.

        Raises:
            Exception: If the request to Spotify API fails.
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
    """
    A client for interacting with the Spotify API to fetch playlist and artist data.

    Attributes:
        access_token_client (SpotifyAccessTokenClient): An instance for managing access tokens.
        access_token (str): The current access token for authenticating API requests.
    """

    def __init__(self, access_token_client: SpotifyAccessTokenClient):
        """
        Initialize the SpotifyAPIClient with an instance of SpotifyAccessTokenClient.

        Args:
            access_token_client (SpotifyAccessTokenClient): An instance responsible for retrieving access tokens.
        """
        self.access_token_client = access_token_client
        self.access_token = self.access_token_client.get_access_token()

    def get_playlist_data(self, playlist_id: str) -> tuple[dict, list[dict]]:
        """
        Retrieve playlist metadata and all tracks within the playlist, with pagination support.

        Spotify's API returns track data in pages (batches). This method handles pagination to 
        gather all tracks within the playlist.

        Args:
            playlist_id (str): The Spotify playlist ID.

        Returns:
            tuple: 
                dict: The playlist metadata.
                list[dict]: A list of all tracks within the playlist.

        Raises:
            Exception: If there is an error with the Spotify API request.
        """
        playlist_url = f"https://api.spotify.com/v1/playlists/{playlist_id}"
        headers = {'Authorization': f"Bearer {self.access_token}"}
        params = {'limit': 100}

        # Initial request to get playlist metadata and first page of tracks
        response = requests.get(playlist_url, headers=headers, params=params)
        if response.status_code != 200:
            raise Exception(
                f"Error fetching playlist data: {response.status_code}")

        playlist_data = response.json()

        # Store all track data
        all_tracks = playlist_data['tracks']['items']

        # Handle pagination if more tracks exist beyond the first page
        while playlist_data['tracks']['next']:
            response = requests.get(
                playlist_data['tracks']['next'], headers=headers)
            if response.status_code != 200:
                raise Exception(
                    f"Error fetching next page of tracks: {response.status_code}")
            playlist_data['tracks'] = response.json()
            all_tracks.extend(playlist_data['tracks']['items'])

        return playlist_data, all_tracks

    def get_artist(self, artist_id: str) -> dict:
        """
        Retrieve data for a specific artist by their Spotify artist ID.

        Args:
            artist_id (str): The Spotify artist ID.

        Returns:
            dict: The artist's data.

        Raises:
            Exception: If there is an error with the Spotify API request.
        """
        artist_url = f"https://api.spotify.com/v1/artists/{artist_id}"
        headers = {'Authorization': f"Bearer {self.access_token}"}

        response = requests.get(artist_url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                f"Error fetching artist data: {response.status_code}")
