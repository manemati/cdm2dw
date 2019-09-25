using System;
using System.IO;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using System.Web;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.CdmFolders.SampleLibraries;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;


namespace cdm2dw
{
    public static class Parse
    {

        [FunctionName("Parse")]
        public static async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = null)] HttpRequest req, ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            string name = req.Query["name"];

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);

            string modelJSON = data["model"].ToString();
            dynamic dataTypeMap = data["dataTypeMap"] as JObject;

            JArray results = new JArray();
            Model model = new Model();

            try
            {
                model.FromJson(modelJSON);
                model.ValidateModel();

                foreach (LocalEntity entity in model.Entities)
                {
                    JObject entityJObject = new JObject();

                    string createTableQuery = "Create Table {0}";
                    StringBuilder columns = new StringBuilder();
                    string tableName = string.Format("[{0}]", entity.Name.Replace(" ", string.Empty));

                    string modifiedTimeJPath = string.Format("$..entities[?(@.name == '{0}')].modifiedTime", entity.Name);
                    IsoDateTimeConverter dateTimeConvertor = new IsoDateTimeConverter()
                    {
                        DateTimeStyles = System.Globalization.DateTimeStyles.AdjustToUniversal,
                    };

                    string entityModifiedTime = data.SelectToken(modifiedTimeJPath)?.ToString(Formatting.None, dateTimeConvertor).Replace("\"", "");

                    JArray tableStructure = new JArray();
                    foreach (var attr in entity.Attributes)
                    {
                        string columnType = attr.DataType.ToString();
                        if (!dataTypeMap.TryGetValue(columnType, StringComparison.OrdinalIgnoreCase, out JToken sqlDwTypeToken))
                        {
                            throw new Exception($"No type mapping found for {columnType}");
                        }
                        columns.Append(string.Format("[{0}] {1},", attr.Name, sqlDwTypeToken.ToString()));

                        tableStructure.Add(new JObject
                        {
                            { "name", attr.Name },
                            { "type", columnType }
                        });

                        JObject destColumnStructure = new JObject();
                        destColumnStructure.Add("name", attr.Name);
                        destColumnStructure.Add("type", columnType);
                        tableStructure.Add(destColumnStructure);
                    }


                    string columnsQuery = columns.ToString();
                    createTableQuery = string.Format(createTableQuery, tableName, columnsQuery.TrimEnd(','));

                    JArray dataFileLocationsArray = new JArray();
                    foreach (var partition in entity.Partitions)
                    {
                        string relativePath = HttpUtility.UrlDecode(partition.Location.AbsolutePath);
                        string folderPath = relativePath.Substring(1, relativePath.LastIndexOf('/'));
                        string filePath = relativePath.Substring(relativePath.LastIndexOf('/') + 1);

                        JObject datafileLocation = new JObject();
                        datafileLocation.Add("folderPath", folderPath);
                        datafileLocation.Add("filePath", filePath);
                        datafileLocation.Add("refreshTime", partition.RefreshTime.Value.DateTime.ToUniversalTime());
                        dataFileLocationsArray.Add(datafileLocation);
                    }

                    entityJObject.Add("name", entity.Name);
                    entityJObject.Add("tableName", tableName);
                    entityJObject.Add("modifiedTime", entityModifiedTime);
                    entityJObject.Add("tableStructure", tableStructure);
                    entityJObject.Add("query", createTableQuery);
                    entityJObject.Add("datafileLocations", dataFileLocationsArray);

                    results.Add(entityJObject);
                }

                JObject response = new JObject();
                response.Add("result", results);

                return response != null
                    ? (ActionResult)new OkObjectResult("success")
                    : new BadRequestObjectResult(HttpStatusCode.BadRequest);
            }
            catch (Exception e)
            {

                return req != null
                   ? (ActionResult)new OkObjectResult("failed")
                   : new BadRequestObjectResult(HttpStatusCode.BadRequest);



            }           
        }
    }
}

