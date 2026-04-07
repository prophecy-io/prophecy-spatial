import dataclasses
import json
from dataclasses import dataclass, field

from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *

class Buffer(MacroSpec):
    name: str = "Buffer"
    projectName: str = "prophecy_spatial"
    category: str = "Spatial"
    minNumOfInputPorts: int = 1
    supportedProviderTypes: list[ProviderTypeEnum] = [
        ProviderTypeEnum.Databricks,
        # ProviderTypeEnum.Snowflake,
        # ProviderTypeEnum.BigQuery,
        # ProviderTypeEnum.ProphecyManaged
    ]
    
    @dataclass(frozen=True)
    class BufferProperties(MacroProperties):
        # properties for the component with default values
        relation_name: List[str] = field(default_factory=list)
        schema: str = ''
        distance: int = 1
        unit: str = "miles"
        geometryColumnName: str = ""
        

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

    @staticmethod
    def _sql_string_literal(val) -> str:
        """Single SQL string literal for macro args (GenerateRows / Regex pattern for schema)."""
        if val is None or val == "":
            return "''"
        if isinstance(val, str):
            return "'" + val.replace("'", "''") + "'"
        if isinstance(val, list):
            return "'" + str(val).replace("'", "''") + "'"
        return "'" + str(val).replace("'", "''") + "'"

    @staticmethod
    def _strip_sql_string_literal(raw) -> str:
        """Inverse of _sql_string_literal: requires a single-quoted SQL string; otherwise ''."""
        if raw is None or raw == "":
            return ""
        s = str(raw).strip()
        if len(s) >= 2 and s[0] == "'" and s[-1] == "'":
            return s[1:-1].replace("''", "'")
        return ""

    @classmethod
    def _parse_table_name_list(cls, raw_rel) -> List[str]:
        if raw_rel is None or str(raw_rel).strip() == "":
            return []
        parsed = json.loads(str(raw_rel).replace("'", '"'))
        if isinstance(parsed, list):
            return [str(x) for x in parsed]
        return [str(parsed)]

    def dialog(self) -> Dialog:
        return Dialog("Buffer").addElement(
            ColumnsLayout(gap="1rem", height="100%")
            .addColumn(
                Ports(allowInputAddOrDelete=True),
                "content"
            )
            .addColumn(
                StackLayout()
                .addElement(
                   AlertBox(
                       variant="warning",
                       _children=[
                           Markdown(
                               "**This Gem uses Databricks Spatial SQL features currently in Private Preview.**\n\n"
                               "To enable these capabilities, please contact your Databricks representative. For more information, see the [Databricks Preview Feature Documentation](https://docs.databricks.com/en/admin/workspace-settings/manage-previews.html)."
                            )
                       ]
                   )   
                )  
                .addElement(
                    SchemaColumnsDropdown("Geometry column")
                        .bindSchema("component.ports.inputs[0].schema")
                        .bindProperty("geometryColumnName")
                )                               
                .addElement(
                    NumberBox("Distance",placeholder="10").bindProperty("distance")
                )                
                .addElement(
                    SelectBox("Units").addOption("Miles", "miles").addOption("Kilometers", "kms").bindProperty("unit")
                )  
       ))

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        # Validate the component's state
        return super().validate(context,component)

    def onChange(self, context: SqlContext, oldState: Component, newState: Component) -> Component:
        # Handle changes in the component's state and return the new state
        schema = json.loads(str(newState.ports.inputs[0].schema).replace("'", '"'))
        fields_array = [
            {"name": sf["name"], "dataType": sf["dataType"]["type"]}
            for sf in schema["fields"]
        ]
        relation_name = self.get_relation_names(newState, context)

        newProperties = dataclasses.replace(
            newState.properties,
            schema=json.dumps(fields_array),
            relation_name=relation_name
        )
        return newState.bindProperties(newProperties)

    def apply(self, props: BufferProperties) -> str:

        # generate the actual macro call given the component's
        resolved_macro_name = f"{self.projectName}.{self.name}"

        arguments = [
            json.dumps(props.relation_name),
            self._sql_string_literal(props.schema),
            self._sql_string_literal(props.geometryColumnName),
            str(props.distance),
            self._sql_string_literal(props.unit),
        ]

        params = ",".join([param for param in arguments])
        return f'{{{{ {resolved_macro_name}({params}) }}}}'

    # -------------------------------------------------------------------------
    # Property loading/unloading
    # -------------------------------------------------------------------------
    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        # load the component's state given default macro property representation
        parametersMap = self.convertToParameterMap(properties.parameters)
        # Macro: Buffer(table_name, schema, geom_column_name, distance, unit)
        relation_name = self._parse_table_name_list(parametersMap.get("table_name"))

        schema_raw = parametersMap.get("schema")
        geom_raw = parametersMap.get("geom_column_name")
        unit_raw = parametersMap.get("unit")
        dist_raw = parametersMap.get("distance")
        try:
            distance = int(float(dist_raw)) if dist_raw not in (None, "") else 1
        except (TypeError, ValueError):
            distance = 1

        schema_val = self._strip_sql_string_literal(schema_raw)
        geometry_col = self._strip_sql_string_literal(geom_raw)
        unit_val = self._strip_sql_string_literal(unit_raw)

        return Buffer.BufferProperties(
            relation_name=relation_name,
            schema=schema_val,
            geometryColumnName=geometry_col,
            distance=distance,
            unit=unit_val,
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        # convert component's state to default macro property representation
        # Match apply() argument strings exactly (same idea as CreatePoint matchFields ↔ json.dumps).
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("table_name", json.dumps(properties.relation_name)),
                MacroParameter("schema", self._sql_string_literal(properties.schema)),
                MacroParameter(
                    "geom_column_name",
                    self._sql_string_literal(properties.geometryColumnName),
                ),
                MacroParameter("distance", str(properties.distance)),
                MacroParameter("unit", self._sql_string_literal(properties.unit)),
            ],
        )

    def updateInputPortSlug(self, component: Component, context: SqlContext):
        schema = json.loads(str(component.ports.inputs[0].schema).replace("'", '"'))
        fields_array = [
            {"name": sf["name"], "dataType": sf["dataType"]["type"]}
            for sf in schema["fields"]
        ]
        relation_name = self.get_relation_names(component, context)

        newProperties = dataclasses.replace(
            component.properties,
            schema=json.dumps(fields_array),
            relation_name=relation_name
        )
        return component.bindProperties(newProperties)
