namespace DynamoDB.ClientWrapper
{
    using System;
    
    public class GetDataException : Exception
    {
        public GetDataException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}