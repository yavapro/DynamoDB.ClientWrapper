namespace DynamoDB.ClientWrapper.Tests
{
    using FluentAssertions;
    
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    
    using Xunit;
    
    public class InMemoryStubDynamoDbProviderTests
    {
        private readonly InMemoryStubDynamoDbProvider target;
        private const string tableName = "Test";
        private const string primaryKeyName = "Id";

        public InMemoryStubDynamoDbProviderTests()
        {
            target = new InMemoryStubDynamoDbProvider(tableName, new Dictionary<string, string>());
        }

        [Fact]
        public async Task PutItemAsync_Success_Test()
        {
            var data = new TestData
            {
                Id = Guid.NewGuid().GetHashCode(),
                Value = Guid.NewGuid().GetHashCode()
            };
            
            await target.PutItemAsync(tableName, primaryKeyName, data);
        }
        
        [Fact]
        public async Task PutItemAsync_FailDuplicate_Test()
        {
            var data = new TestData
            {
                Id = Guid.NewGuid().GetHashCode(),
                Value = Guid.NewGuid().GetHashCode()
            };
            
            await target.PutItemAsync(tableName, primaryKeyName, data);
            
            await Assert.ThrowsAsync<DuplicateKeyException>(
                () =>
                    target.PutItemAsync(tableName, primaryKeyName, data, true));
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
                    target.PutItemAsync(Guid.NewGuid().ToString(), primaryKeyName, data));
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
                    target.PutItemAsync(tableName, primaryKeyName, data));
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
            
            await target.PutItemAsync(tableName, primaryKeyName, data);
            
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
        public async Task GetBatchItemsAsync_FailObjectType_Test()
        {
            var data = new TestData
            {
                Id = Guid.NewGuid().GetHashCode(),
                Value = Guid.NewGuid().GetHashCode()
            };
            
            await target.PutItemAsync(tableName, primaryKeyName, data);
            
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