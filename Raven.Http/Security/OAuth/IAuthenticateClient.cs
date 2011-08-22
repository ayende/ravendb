﻿namespace Raven.Http.Security.OAuth
{
    public interface IAuthenticateClient
    {
        bool Authenticate(string clientId, string clientSecret);
    }
}