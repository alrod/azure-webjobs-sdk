// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Microsoft.Azure.WebJobs.Extensions.Polly
{
    public static class PollyWebJobsBuilderExtensions
    {
        public static IWebJobsBuilder AddPollyRetryManager(this IWebJobsBuilder builder)
        {
            builder.Services.TryAddSingleton<IRetryManagerProvider, PollyRetryManagerProvider>();

            return builder;
        }
    }
}
