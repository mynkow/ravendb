using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Connection.Async;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.SlowTests.Issues;
using Raven.Tests.Common;
using Raven.Tests.Core;
using Raven.Tests.Core.Querying;
using Raven.Tests.Issues;
using Raven.Tests.Spatial.JsonConverters.GeoJson;

namespace Raven.Tryouts
{
	public class Program
	{
		private static void Main()
		{
            BulkInsertsPerformance.Main();
		}
	}

}