using System;
// [START datastore_quickstart]
using Google.Cloud.BigQuery.V2;

namespace aspnetcore_bigquery_console
{
    public class QuickStart
    {
        public static void Main(string[] args)
        {
            // [START bigquery_simple_app_client]
            string projectId = "aspnet-bigquery-sample";
            var client = BigQueryClient.Create(projectId);
            
            // [END bigquery_simple_app_client]
            // [START bigquery_simple_app_query]
            string query = @"SELECT
                CONCAT(
                    'https://stackoverflow.com/questions/',
                    CAST(id as STRING)) as url, view_count
                FROM `bigquery-public-data.stackoverflow.posts_questions`
                WHERE tags like '%google-bigquery%'
                ORDER BY view_count DESC
                LIMIT 10";
            var result = client.ExecuteQuery(query, parameters: null);
            // [END bigquery_simple_app_query]
            // [START bigquery_simple_app_print]
            Console.Write("\nQuery Results:\n------------\n");
            foreach (var row in result)
            {
                Console.WriteLine($"{row["url"]}: {row["view_count"]} views");
            }

            Console.ReadLine();
            // [END bigquery_simple_app_print]
        }
    }
    // [END datastore_quickstart]
}