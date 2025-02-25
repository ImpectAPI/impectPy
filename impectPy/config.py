class Config(object):
    def __init__(self, host: str = 'https://api.impect.com', oidc_token_endpoint: str = 'https://login.impect.com/auth/realms/production/protocol/openid-connect/token'):
        self.HOST = host
        self.OIDC_TOKEN_ENDPOINT = oidc_token_endpoint