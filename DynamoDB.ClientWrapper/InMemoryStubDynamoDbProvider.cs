using System;

namespace DynamoDB.ClientWrapper
{
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using System.Linq;
    using System.Reflection;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public class InMemoryStubDynamoDbProvider : IDynamoDbProvider
    {
        private readonly IDictionary<string, IDictionary<string, string>> data;
        private readonly IDictionary<string, IEnumerable<string>> keys;
        private readonly object locking = new object();

        public InMemoryStubDynamoDbProvider(string tableName, IEnumerable<string> keyNames,
            IDictionary<string, string> tableStorage)
        {
            data = new Dictionary<string, IDictionary<string, string>>();
            keys = new Dictionary<string, IEnumerable<string>>();
            data.Add(tableName, tableStorage);
            keys.Add(tableName, keyNames);
        }

        public Task PutItemAsync(string tableName, object item, bool checkUniqueKey = false)
        {
            if (!data.ContainsKey(tableName))
            {
                throw new TableNameFailException($"Not found the source '{tableName}'.", null);
            }

            var keyName = keys[tableName].First();

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

        public Task UpdateItemAsync(string tableName, object item)
        {
            if (!data.ContainsKey(tableName))
            {
                throw new TableNameFailException($"Not found the source '{tableName}'.", null);
            }

            var keyName = keys[tableName].First();

            var property = item.GetType().GetProperty(keyName, BindingFlags.Public | BindingFlags.Instance);

            if (property == null)
            {
                throw new PrimaryKeyNameFailException($"Not found the primary key in saving data.", null);
            }

            var jsonDataItem = JsonConvert.SerializeObject(item);
            var val = property.GetValue(item);
            var itemKey = $"{keyName}-{val}";

            if (!data[tableName].ContainsKey(itemKey))
            {
                throw new NotExistKeyException(
                    $"The source '{tableName}' has not contained data with keys '{keyName}'.",
                    null);
            }

            var savedItem = JObject.Parse(data[tableName][itemKey]);
            var newItem = JObject.Parse(jsonDataItem);

            savedItem.Merge(newItem);
            data[tableName][itemKey] = savedItem.ToString();

            return Task.CompletedTask;
        }

        public Task<IEnumerable<TObject>> GetBatchItemsAsync<TObject>(string tableName,
            IEnumerable<object> keyValues)
        {
            var keyValuesDictionary = keyValues.Select(k => ToDictionary(k)).ToArray();

            if (!data.ContainsKey(tableName))
            {
                throw new TableNameFailException($"Not found the source '{tableName}'.", null);
            }

            var keyName = keys[tableName].First();

            var keyItems = keyValuesDictionary.Select(e => $"{keyName}-{e[keyName]}");
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
                catch (JsonReaderException e)
                {
                    throw new IncorrectDataFormatException(
                        $"'{typeof(TObject).Name}' and object '{jsonDataItem}' have a different data format",
                        e);
                }

                items.Add(data);
            }

            return Task.FromResult(items.AsEnumerable());
        }

        private Dictionary<string, object> ToDictionary(object obj)
        {
            return JObject.FromObject(obj).ToObject<Dictionary<string, object>>();
        }
    }
}