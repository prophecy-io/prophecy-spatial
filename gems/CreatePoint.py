import dataclasses
from collections import defaultdict
from prophecy.cb.sql.Component import *
from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *
import json

class MatchField(ABC):
    pass


class CreatePoint(MacroSpec):
    name: str = "CreatePoint"
    projectName: str = "prophecy_spatial"
    category: str = "Spatial"
    minNumOfInputPorts: int = 1
    supportedProviderTypes: list[ProviderTypeEnum] = [
        ProviderTypeEnum.Databricks,
        # ProviderTypeEnum.Snowflake,
        # ProviderTypeEnum.BigQuery,
        ProviderTypeEnum.ProphecyManaged
    ]

    @dataclass(frozen=True)
    class AddMatchField(MatchField):
        longitudeColumnName: str = ""
        latitudeColumnName: str = ""
        targetColumnName: str = ""

    @dataclass(frozen=True)
    class CreatePointProperties(MacroProperties):
        addFields: List[MatchField] = field(default_factory=list)
        relation_name: List[str] = field(default_factory=list)

    def get_relation_names(self, component: Component, context: SqlContext):
        all_upstream_nodes = []
        for inputPort in component.ports.inputs:
            upstreamNode = None
            for connection in context.graph.connections:
                if connection.targetPort == inputPort.id:
                    upstreamNodeId = connection.source
                    upstreamNode = context.graph.nodes.get(upstreamNodeId)
            all_upstream_nodes.append(upstreamNode)

        relation_name = []
        for upstream_node in all_upstream_nodes:
            if upstream_node is None or upstream_node.label is None:
                relation_name.append("")
            else:
                relation_name.append(upstream_node.label)

        return relation_name

    def onButtonClick(self, state: Component[CreatePointProperties]):
        _addFields = state.properties.addFields
        _addFields.append(self.AddMatchField())
        return state.bindProperties(dataclasses.replace(state.properties, addFields=_addFields))

    def dialog(self) -> Dialog:
        addFields = StackLayout(gap=("1rem"), height=("100bh")) \
            .addElement(TitleElement("Create Spatial Points")) \
            .addElement(
            AlertBox(
                variant="success",
                _children=[
                    Markdown(
                        "Please add Latitude,Longitude and Target Column Name as Pair"
                        "\n"
                        "* **Longitude Column Name** - Column containing Longitude values \n"
                        "* **Latitude Column Name** - Column containing Latitude values \n"
                        "* **Target Column Name** - Target column name to keep transformed Geo Spatial Data \n"
                    )
                ]
            )
        ) \
            .addElement(
            StepContainer()
                .addElement(
                Step()
                    .addElement(
                    OrderedList("Add Fields")
                        .bindProperty("addFields")
                        .setEmptyContainerText("Add a Point")
                        .addElement(
                        ColumnsLayout(("1rem"), alignY=("end"))
                            .addColumn(
                            ColumnsLayout("1rem")
                                .addColumn(
                                SchemaColumnsDropdown("Longitude Column Name")
                                    .bindSchema("component.ports.inputs[0].schema")
                                    .bindProperty("record.AddMatchField.longitudeColumnName")
                                , "0.5fr")
                                .addColumn(
                                SchemaColumnsDropdown("Latitude Column Name")
                                    .bindSchema("component.ports.inputs[0].schema")
                                    .bindProperty("record.AddMatchField.latitudeColumnName")
                                , "0.5fr")
                                .addColumn(
                                TextBox("Target Column Name").bindPlaceholder("")
                                    .bindProperty("record.AddMatchField.targetColumnName")
                                , "0.5fr")
                        )
                            .addColumn(ListItemDelete("delete"), width="content")
                    )
                )
            )
        ) \
            .addElement(SimpleButtonLayout("Click to Add a Point", self.onButtonClick))

        return Dialog("CreatePoint") \
            .addElement(
            ColumnsLayout(gap=("1rem"), height=("100%"))
                .addColumn(
                Ports(), "content"
            )
                .addColumn(addFields)
        )

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        # Validate the component's state
        diagnostics = super(CreatePoint, self).validate(context, component)

        if len(component.properties.addFields) == 0:
            diagnostics.append(
                Diagnostic("component.properties.addFields", "Please add atleast one point", SeverityLevelEnum.Error))

        # Check 2: If fields are having only numeric fields
        numeric_types = {"int", "integer", "float", "double", "long", "decimal", "bigint", "smallint", "tinyint"}

        # Step 1: Load and parse the schema
        schema = json.loads(str(component.ports.inputs[0].schema).replace("'", '"'))
        type_lookup = {field["name"]: field["dataType"]["type"].lower() for field in schema["fields"]}

        # Step 2: Iterate through fields and check if the longitude column is numeric
        for field in component.properties.addFields:
            if field.longitudeColumnName and type_lookup.get(field.longitudeColumnName, "").lower() not in numeric_types:
                diagnostics.append(
                    Diagnostic("component.properties.addFields", "Please give a longitude field with numeric data type", SeverityLevelEnum.Error))

            if field.latitudeColumnName and type_lookup.get(field.latitudeColumnName, "").lower() not in numeric_types:
                diagnostics.append(
                    Diagnostic("component.properties.addFields", "Please give a latitude field with numeric data type", SeverityLevelEnum.Error))

        # Check 2: Null checks for fields
        for field in component.properties.addFields:
            if field.longitudeColumnName == "":
                diagnostics.append(
                    Diagnostic("component.properties.addFields", "Please select the longitude column name", SeverityLevelEnum.Error))
            if field.latitudeColumnName == "":
                diagnostics.append(
                    Diagnostic("component.properties.addFields", "Please select the latitude column name", SeverityLevelEnum.Error))
            if field.targetColumnName == "":
                diagnostics.append(
                    Diagnostic("component.properties.addFields", "Please provide a target column name", SeverityLevelEnum.Error))

        # Check 3: If schema is updated but not selected fields
        # Extract all column names from the schema
        field_names = [field["name"] for field in component.ports.inputs[0].schema["fields"]]

        # Extract longitude column names from addFields
        match_longitude_field = [field.longitudeColumnName for field in component.properties.addFields if field.longitudeColumnName]
        # Identify missing columns
        missing_match_longitude_columns = [col for col in match_longitude_field if col not in field_names]

        # Append diagnostic if any are missing
        if missing_match_longitude_columns:
            diagnostics.append(
                Diagnostic(
                    "component.properties.addFields",
                    f"Selected matchField columns {missing_match_longitude_columns} is not present in input schema.",
                    SeverityLevelEnum.Error
                )
            )

        # Extract latitude column names from addFields
        match_latitude_field = [field.latitudeColumnName for field in component.properties.addFields if field.latitudeColumnName]
        # Identify missing columns
        missing_match_latitude_columns = [col for col in match_latitude_field if col not in field_names]

        # Append diagnostic if any are missing
        if missing_match_latitude_columns:
            diagnostics.append(
                Diagnostic(
                    "component.properties.addFields",
                    f"Selected matchField columns {missing_match_latitude_columns} is not present in input schema.",
                    SeverityLevelEnum.Error
                )
            )

        return diagnostics

    def onChange(self, context: SqlContext, oldState: Component, newState: Component) -> Component:
        # Handle changes in the component's state and return the new state
        relation_name = self.get_relation_names(newState, context)
        return (replace(newState, properties=replace(newState.properties, relation_name=relation_name)))

    def apply(self, props: CreatePointProperties) -> str:
        # generate the actual macro call given the component's state
        resolved_macro_name = f"{self.projectName}.{self.name}"

        # Get the Single Table Name
        table_name: str = ",".join(str(rel) for rel in props.relation_name)

        # Group fields
        grouped_fields = []
        for field in props.addFields:
            grouped_fields.append([field.longitudeColumnName,field.latitudeColumnName,field.targetColumnName])

        arguments = [
            "'" + table_name + "'",
            str(grouped_fields)
        ]
        params = ",".join([param for param in arguments])
        return f'{{{{ {resolved_macro_name}({params}) }}}}'

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        # load the component's state given default macro property representation
        parametersMap = self.convertToParameterMap(properties.parameters)
        return CreatePoint.CreatePointProperties(
            relation_name=parametersMap.get('relation_name')
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        # convert component's state to default macro property representation
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation_name", str(properties.relation_name))
            ],
        )

    def updateInputPortSlug(self, component: Component, context: SqlContext):
        relation_name = self.get_relation_names(component, context)
        return (replace(component, properties=replace(component.properties, relation_name=relation_name)))