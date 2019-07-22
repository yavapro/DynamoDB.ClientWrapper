namespace DynamoDB.ClientWrapper
{
    using System;

    public class IncorrectDataFormatException : Exception
    {
        public IncorrectDataFormatException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}