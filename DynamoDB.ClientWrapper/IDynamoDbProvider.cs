namespace DynamoDB.ClientWrapper
{
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public interface IDynamoDbProvider
    {
        Task PutItemAsync(string tableName, object item, bool checkUniqueKey = false);

        Task UpdateItemAsync(string tableName, object item);

        Task<IEnumerable<TObject>> GetBatchItemsAsync<TObject>(string tableName, IEnumerable<object> keyValues);
    }
}