namespace DynamoDB.ClientWrapper.Tests
{
    public class Query
    {
        public int Id { get; set; }

        public static Query Empty => new Query();
    }
}