[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "orders_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "SalesOrderID", DestinationColumnName = "SalesOrderID"], [SourceColumnName = "OrderDate", DestinationColumnName = "OrderDate"], [SourceColumnName = "CustomerID", DestinationColumnName = "CustomerID"], [SourceColumnName = "LineItem", DestinationColumnName = "LineItem"], [SourceColumnName = "ProductID", DestinationColumnName = "ProductID"], [SourceColumnName = "OrderQty", DestinationColumnName = "OrderQty"], [SourceColumnName = "LineItemTotal", DestinationColumnName = "LineItemTotal"], [SourceColumnName = "MonthNo", DestinationColumnName = "MonthNo"]}], DynamicSchema = false, UpdateMethod = [Kind = "Append"], TypeSettings = [Kind = "Table"]]]}]
shared orders = let
  Source = Csv.Document(Web.Contents("https://raw.githubusercontent.com/MicrosoftLearning/dp-data/main/orders.csv"), [Delimiter = ",", Columns = 7, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"SalesOrderID", Int64.Type}, {"OrderDate", type date}, {"CustomerID", Int64.Type}, {"LineItem", Int64.Type}, {"ProductID", Int64.Type}, {"OrderQty", Int64.Type}, {"LineItemTotal", type number}}),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Changed column type", "MonthNo", each Date.Month([OrderDate])), {{"MonthNo", Int64.Type}})
in
  #"Added custom";
shared orders_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "882888b1-bd44-4d16-bdaa-29b9dca4a75e"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "c229aeca-e716-409e-9d92-380f97583051"]}[Data],
  Navigation_3 = Navigation_2{[Id = "dbo", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "orders", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
