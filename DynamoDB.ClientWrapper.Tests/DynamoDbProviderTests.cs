namespace DynamoDB.ClientWrapper.Tests
{
    using Amazon.DynamoDBv2;
    using Microsoft.Extensions.Configuration;
    
    using FluentAssertions;
    
    using System;
    using System.Linq;
    using System.Threading.Tasks;
    
    using Xunit;
    
    public class DynamoDbProviderTests
    {
        private readonly DynamoDbProvider target;
        private const string tableName = "Test";
        private const string primaryKeyName = "Id";

        public DynamoDbProviderTests()
        {
            var awsOptions = Configuration.Current.GetAWSOptions();
            var dynamoDbClient = awsOptions.CreateServiceClient<IAmazonDynamoDB>();
            
            target = new DynamoDbProvider(dynamoDbClient);
        }

        [Fact]
        public async Task PutItemAsync_Success_Test()
        {
            var data = new TestData
            {
                Id = Guid.NewGuid().GetHashCode(),
                Value = Guid.NewGuid().GetHashCode()
            };
            
            await target.PutItemAsync(tableName, data);
        }
        
        [Fact]
        public async Task PutItemAsync_FailDuplicate_Test()
        {
            var data = new TestData
            {
                Id = Guid.NewGuid().GetHashCode(),
                Value = Guid.NewGuid().GetHashCode()
            };
            
            await target.PutItemAsync(tableName, data);
            
            await Assert.ThrowsAsync<DuplicateKeyException>(
                () =>
                    target.PutItemAsync(tableName, data, primaryKeyName));
        }
        
        [Fact]
        public async Task PutItemAsync_FailTableName_Test()
        {
            var data = new TestData
            {
                Id = Guid.NewGuid().GetHashCode(),
                Value = Guid.NewGuid().GetHashCode()
            };
            
            await Assert.ThrowsAsync<TableNameFailException>(
                () =>
                    target.PutItemAsync(Guid.NewGuid().ToString(), data));
        }
        
        [Fact]
        public async Task PutItemAsync_FailKeyName_Test()
        {
            var data = new
            {
                Value = Guid.NewGuid().GetHashCode()
            };
            
            await Assert.ThrowsAsync<PrimaryKeyNameFailException>(
                () =>
                    target.PutItemAsync(tableName, data));
        }
        
        [Fact]
        public async Task PutItemAsync_FailServiceEndpoint_Test()
        {
            var clientConfig = new AmazonDynamoDBConfig();
            clientConfig.ServiceURL = "http://localhost:1000";
            var client = new AmazonDynamoDBClient(clientConfig);
            var provider = new DynamoDbProvider(client);
            
            var data = new TestData
            {
                Id = Guid.NewGuid().GetHashCode(),
                Value = Guid.NewGuid().GetHashCode()
            };
            
            await Assert.ThrowsAsync<SaveDataException>(
                () =>
                    provider.PutItemAsync(tableName, data));
        }
        
        [Fact]
        public async Task GetBatchItemsAsync_CheckEmptyResult_Test()
        {
            var data = new TestData
            {
                Id = Guid.NewGuid().GetHashCode(),
                Value = Guid.NewGuid().GetHashCode()
            };
            
            var responce = await target.GetBatchItemsAsync<TestData, int>(tableName, primaryKeyName, new []{ 0 });
            
            Assert.NotNull(responce);
            Assert.Empty(responce);
        }
        
        [Fact]
        public async Task GetBatchItemsAsync_CheckGetSameResult_Test()
        {
            var data = new TestData
            {
                Id = Guid.NewGuid().GetHashCode(),
                Value = Guid.NewGuid().GetHashCode()
            };
            
            await target.PutItemAsync(tableName, data);
            
            var responce = await target.GetBatchItemsAsync<TestData, int>(tableName, primaryKeyName, new []{ data.Id });
            
            Assert.NotNull(responce);
            Assert.NotEmpty(responce);
            Assert.Single(responce);
            responce.First().Should().BeEquivalentTo(data);
        }
        
        [Fact]
        public async Task GetBatchItemsAsync_FailTableName_Test()
        {
            
            await Assert.ThrowsAsync<TableNameFailException>(
                () =>
                    target.GetBatchItemsAsync<TestData, int>(Guid.NewGuid().ToString(), primaryKeyName, new int[] { 0 }));
        }
        
        [Fact]
        public async Task GetBatchItemsAsync_FailKeyName_Test()
        {
            
            await Assert.ThrowsAsync<PrimaryKeyNameFailException>(
                () =>
                    target.GetBatchItemsAsync<TestData, int>(tableName, "NotExistsKey", new int[] { 0 }));
        }
        
        [Fact]
        public async Task GetBatchItemsAsync_FailKeyName2_Test()
        {
            var data = new TestData
            {
                Id = Guid.NewGuid().GetHashCode(),
                Value = Guid.NewGuid().GetHashCode()
            };
            
            await target.PutItemAsync(tableName, data);
            
            await Assert.ThrowsAsync<IncorrectDataFormatException>(
                () =>
                    target.GetBatchItemsAsync<Data, int>(tableName, primaryKeyName, new int[] { data.Id }));
        }
        
        private class Data
        {
            public DateTime Value { get; set; }
        }
    }
}