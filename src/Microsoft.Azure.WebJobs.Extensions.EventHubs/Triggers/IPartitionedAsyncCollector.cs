// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs.Host.Bindings;
using Microsoft.Azure.WebJobs.Host.Config;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Azure.WebJobs.EventHubs
{
    /// <summary>
    /// Defines a method to send partition key
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface IPartitionedAsyncCollector<T>
    {
        Task AddAsync(T item, string partitionKey, CancellationToken cancellationToken = default(CancellationToken));
    }

    public class PartitionedAsyncCollector
    {
        private IConverterManager _converterManager;
        private EventHubAttribute _eventHubAttribute;
        private EventHubClient _client;

        public PartitionedAsyncCollector(EventHubClient client, IConverterManager converterManager, EventHubAttribute eventHubAttribute)
        {
            _client = client;
            _converterManager = converterManager;
            _eventHubAttribute = eventHubAttribute;
        }

        public async Task AddAsync<T>(T item, string partitionKey, CancellationToken cancellationToken = default(CancellationToken))
        {
            FuncAsyncConverter convertor = _converterManager.GetConverter<EventHubAttribute>(typeof(T), typeof(EventData));
            EventData result = await convertor.Invoke(item, _eventHubAttribute, null) as EventData;

            if (string.IsNullOrEmpty(partitionKey))
            {
                await _client.SendAsync(result);
            }
            else
            {
                await _client.SendAsync(result, partitionKey);
            }
        }

        public async Task AddAsync<T>(IEnumerable<T> items, string partitionKey, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (items is IEnumerable<EventData> events)
            {
                await SendAsync(events, partitionKey);
            }

            FuncAsyncConverter convertor = _converterManager.GetConverter<EventHubAttribute>(typeof(T), typeof(EventData));
            List<EventData> list = new List<EventData>();
            foreach (T item in items)
            {
                EventData result = await convertor.Invoke(item, _eventHubAttribute, null) as EventData;
                list.Add(result);
            }
            await SendAsync(list, partitionKey);
        }

        private async Task SendAsync(IEnumerable<EventData> items, string partitionKey)
        {
            if (string.IsNullOrEmpty(partitionKey))
            {
                await _client.SendAsync(items);
            }
            else
            {
                await _client.SendAsync(items, partitionKey);
            }
        }
    }
}
