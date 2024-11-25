class LoadProperties:

    def __init__(self):

        import json
        with open('config/props.txt') as f:
            data = f.read()

        js = json.loads(data)

        self.warehouse_path = js["warehouse_path"]
        self.catalog_name = js["catalog_name"]
        self.catalog_type = js["catalog_type"]
        self.iceberg_spark_jar = js["iceberg_spark_jar"]
        self.table_name = js["table_name"]
        self.input_path = js["input_path"]
        self.mode = js["mode"]
        self.output_dir = js["output_dir"]

    def getWarehousePath(self):
        return self.warehouse_path
    
    def getCatalogName(self):
        return self.catalog_name
    
    def getCatalogType(self):
        return self.catalog_type
    
    def getIcebergSparkJar(self):
        return self.iceberg_spark_jar
    
    def getTableName(self):
        return self.table_name
    
    def getInputPath(self):
        return self.input_path
    
    def getMode(self):
        return self.mode
    
    def getOutputDir(self):
        return self.output_dir