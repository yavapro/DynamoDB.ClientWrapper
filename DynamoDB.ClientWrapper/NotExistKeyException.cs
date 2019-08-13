namespace DynamoDB.ClientWrapper
{
    using System;

    public class NotExistKeyException : Exception
    {
        public NotExistKeyException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}