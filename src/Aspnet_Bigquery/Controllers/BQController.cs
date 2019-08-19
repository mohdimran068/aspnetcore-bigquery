using System;
using System.Collections.Generic;
using System.Linq;
using Aspnet_Bigquery.Bigquery;
using Aspnet_Bigquery.Models;
using Google.Cloud.BigQuery.V2;
using Microsoft.AspNetCore.Mvc;

namespace Aspnet_Bigquery.Controllers
{
    [Route("api/big-query")]
    public class BQController : Controller
    {
        private const int ORDER_ID = 0;
        private const int ORDER_CATEGORY = 1;
        private const int ORDER_STATUS = 2;

        private readonly IBQ _bigQuery;

        public BQController(IBQ bigQuery)
        {
            _bigQuery = bigQuery;
        }

        [HttpPost]
        public IActionResult Post([FromBody]List<File> files)
        {
            var client = _bigQuery.GetBigqueryClient();

            var dataset = client.GetOrCreateDataset("babynames");

            var table = dataset.GetOrCreateTable("tblfile", new TableSchemaBuilder
            {
                { "fileid", BigQueryDbType.String },
                { "category", BigQueryDbType.String },
                { "status", BigQueryDbType.String }
            }.Build());

            var bqRows = new List<BigQueryInsertRow>();
            files.ToList().ForEach(f => bqRows.Add
            (
                new BigQueryInsertRow
                {
                    { "fileid", Guid.NewGuid().ToString() },
                    { "category", f.gender },
                    { "status", f.count }
                }));

            if (bqRows.Count > 1)
                table.InsertRows(bqRows);
            else if (bqRows.Count == 1)
                table.InsertRow(bqRows[0]);

            return Ok($"{bqRows.Count} rows have been added.");
        }

        [HttpGet]
        public IActionResult Get()
        {
            var query = "select * from babynames.names2010";

            var rows = _bigQuery.GetRows(query);

            var result = new List<File>();
            rows.ForEach(row => result.Add(new File
            {
                name = row.F[ORDER_ID].V.ToString(),
                gender = row.F[ORDER_CATEGORY].V.ToString(),
                count = Convert.ToInt32(row.F[ORDER_STATUS].V)
            }));
            return Ok(result);
        }
    }
}
