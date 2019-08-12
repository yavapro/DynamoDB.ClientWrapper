using Newtonsoft.Json;

namespace DynamoDB.ClientWrapper
{
    using System.Linq;
    using System.Reflection;
    
    using System.Collections.Generic;
    using System.Threading.Tasks;
    
    public class InMemoryStubDynamoDbProvider : IDynamoDbProvider
    {
        private readonly IDictionary<string, IDictionary<string, string>> data;
        private readonly object locking = new object();

        public InMemoryStubDynamoDbProvider(string tableName, IDictionary<string, string> tableStorage)
        {
            data = new Dictionary<string, IDictionary<string, string>>();
            data.Add(tableName, tableStorage);
        }

        public Task PutItemAsync<T>(string tableName, string keyName, T item, bool checkUniqueKey = false)
        {
            if (!data.ContainsKey(tableName))
            {
                throw new TableNameFailException($"Not found the source '{tableName}'.", null);
            }
            
            var property = item.GetType().GetProperty(keyName, BindingFlags.Public | BindingFlags.Instance);

            if (property == null)
            {
                throw new PrimaryKeyNameFailException($"Not found the primary key in saving data.", null);
            }
            
            var jsonDataItem = JsonConvert.SerializeObject(item);
            var val = property.GetValue(item);
            var itemKey = $"{keyName}-{val}";

            lock (locking)
            {
                if (data[tableName].ContainsKey(itemKey))
                {
                    if (checkUniqueKey)
                    {
                        throw new DuplicateKeyException(
                        $"The source '{tableName}' has already contained data with key '{keyName}'.",
                        null);
                    }

                    data[tableName][itemKey] = jsonDataItem;
                }
                else
                {
                    data[tableName].Add(itemKey, jsonDataItem);
                }
            }

            return Task.CompletedTask;
        }

        public Task<IEnumerable<TObject>> GetBatchItemsAsync<TObject, TKey>(string tableName, string keyName, IEnumerable<TKey> keyValues)
        {
            if (!data.ContainsKey(tableName))
            {
                throw new TableNameFailException($"Not found the source '{tableName}'.", null);
            }

            var keyItems = keyValues.Select(e => $"{keyName}-{e}");
            IEnumerable<string> jsonDataItems;

            lock (locking)
            {
                jsonDataItems = data[tableName].Where(e => keyItems.Contains(e.Key)).Select(e => e.Value);
            }
            
            var items = new List<TObject>();
            
            foreach (var jsonDataItem in jsonDataItems)
            {
                TObject data;
                
                try
                {
                    data = JsonConvert.DeserializeObject<TObject>(jsonDataItem);
                }
                catch(JsonReaderException e)
                {
                    throw new IncorrectDataFormatException(
                        $"'{typeof(TObject).Name}' and object '{jsonDataItem}' have a different data format", 
                        e);
                }
                
                items.Add(data);
            }

            return Task.FromResult(items.AsEnumerable());
        }
    }
}