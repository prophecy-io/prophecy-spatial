from dataclasses import dataclass, field
import dataclasses
import json

from collections import defaultdict
from prophecy.cb.sql.Component import *
from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *


class Distance(MacroSpec):
    name: str = "Distance"
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
    class DistanceProperties(MacroProperties):
        # properties for the component with default values
        schema: str = ''
        sourceColumnNames: str = ""
        destinationColumnNames: str = ""
        sourceType: str = "point"
        destinationType: str = "point"
        outputDistance: bool = False
        units: str = "kms"
        outputCardDirection: bool = False
        outputDirectionDegrees: bool = False
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

    @staticmethod
    def _sql_string_literal(val) -> str:
        if val is None or val == "":
            return "''"
        return "'" + str(val).replace("'", "''") + "'"

    @staticmethod
    def _strip_sql_string_literal(raw) -> str:
        if raw is None or raw == "":
            return ""
        s = str(raw).strip()
        if len(s) >= 2 and s[0] == "'" and s[-1] == "'":
            return s[1:-1].replace("''", "'")
        return s

    @classmethod
    def _parse_relation_name_list(cls, raw_rel) -> List[str]:
        if raw_rel is None or str(raw_rel).strip() == "":
            return []

        s = str(raw_rel).strip()
        try:
            parsed = json.loads(s.replace("'", '"'))
            if isinstance(parsed, list):
                return [str(x) for x in parsed]
            return [str(parsed)]
        except json.JSONDecodeError:
            return [part.strip() for part in s.split(",") if part.strip()] or [s]

    @classmethod
    def _parse_schema_value(cls, raw_schema, legacy_all_column_names=None) -> str:
        if raw_schema not in (None, ""):
            return cls._strip_sql_string_literal(raw_schema)

        if legacy_all_column_names in (None, ""):
            return "[]"

        s = str(legacy_all_column_names).strip()
        try:
            parsed = json.loads(s.replace("'", '"'))
        except json.JSONDecodeError:
            parsed = []

        if isinstance(parsed, list):
            if all(isinstance(field, dict) for field in parsed):
                return json.dumps(parsed)
            if all(not isinstance(field, dict) for field in parsed):
                return json.dumps([{"name": str(col), "dataType": ""} for col in parsed])

        return "[]"

    def dialog(self) -> Dialog:
        horizontalDivider = HorizontalDivider()
        renameMethod = SelectBox("") \
            .addOption("Edit prefix/suffix", "editPrefixSuffix") \
            .addOption("Advanced rename", "advancedRename") \
            .bindProperty("renameMethod")

        dialog = Dialog("Distance") \
            .addElement(
            ColumnsLayout(gap="1rem", height="100%")
                .addColumn(Ports(), "content")
                .addColumn(
                StackLayout(height="100%")
                    .addElement(
                    StepContainer()
                        .addElement(
                        Step()
                            .addElement(
                            StackLayout(height="100%")
                                .addElement(
                                TitleElement("Spatial Object Fields")
                            )
                                .addElement(
                                ColumnsLayout(gap="1rem", height="100%")
                                    .addColumn(
                                    SelectBox("Source Type").addOption("Point", "point").bindProperty("sourceType")
                                )
                                    .addColumn(
                                    SchemaColumnsDropdown("Source Column")
                                        .bindSchema("component.ports.inputs[0].schema")
                                        .bindProperty("sourceColumnNames")
                                )
                                    .addColumn(
                                    SelectBox("Destination Type").addOption("Point", "point").bindProperty(
                                        "destinationType")
                                )
                                    .addColumn(
                                    SchemaColumnsDropdown("Destination Column")
                                        .bindSchema("component.ports.inputs[0].schema")
                                        .bindProperty("destinationColumnNames")
                                )
                            )
                        )
                    )

                )
                    .addElement(
                    StepContainer()
                        .addElement(
                        Step()
                            .addElement(
                            StackLayout(height="100%")
                                .addElement(TitleElement("Select Output Options"))
                                .addElement(Checkbox("Output Distance").bindProperty("outputDistance"))
                                .addElement(
                                Condition()
                                    .ifEqual(
                                    PropExpr("component.properties.outputDistance"),
                                    BooleanExpr(True),
                                )
                                    .then(
                                    StackLayout(gap=("1rem"), width="20%")
                                        .addElement(
                                        SelectBox("Units").addOption("Kilometers", "kms").addOption("Miles",
                                                                                                    "mls").addOption(
                                            "Feet", "feet").addOption("Meters", "mtr").bindProperty("units")
                                    )
                                )
                            )
                                .addElement(Checkbox("Output Cardinal Direction").bindProperty("outputCardDirection"))
                                .addElement(Checkbox("Output Direction in Degrees").bindProperty("outputDirectionDegrees"))
                        )
                    )
                )
                    .addElement(
                    AlertBox(
                        variant="success",
                        _children=[
                            Markdown(
                                "This gem requires that the Source Column and Destination Column contain geometric values in Well-Known Text (WKT) format. To convert longitude and latitude coordinates into WKT format, use the [CreatePoint gem](https://docs.prophecy.io/analysts/create-point/).\n\n"
                                "Example: If your table has columns like `source_longitude`, `source_latitude`, `target_longitude`, and `target_latitude`, first use the CreatePoint Gem to generate `source_geopoint` and `target_geopoint` columns in WKT format. Then, you can use the Distance gem to calculate the distance between the source and target points."
                            )
                        ]
                    )
                )
            )
        )
        return dialog

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        # Validate the component's state
        diagnostics = super(Distance, self).validate(context, component)

        if len(component.properties.sourceColumnNames) == 0:
            diagnostics.append(
                Diagnostic("component.properties.sourceColumnNames", f"Please select a source column",
                           SeverityLevelEnum.Error)
            )

        if len(component.properties.destinationColumnNames) == 0:
            diagnostics.append(
                Diagnostic("component.properties.destinationColumnNames", f"Please select a destination column",
                           SeverityLevelEnum.Error)
            )

        schema = json.loads(str(component.ports.inputs[0].schema).replace("'", '"'))
        field_names = [field["name"] for field in schema["fields"]]

        if len(component.properties.sourceColumnNames) > 0:
            if component.properties.sourceColumnNames not in field_names:
                diagnostics.append(
                    Diagnostic("component.properties.sourceColumnNames",
                               f"Selected recordId column {component.properties.sourceColumnNames} is not present in input schema.",
                               SeverityLevelEnum.Error))

        if len(component.properties.destinationColumnNames) > 0:
            if component.properties.destinationColumnNames not in field_names:
                diagnostics.append(
                    Diagnostic("component.properties.destinationColumnNames",
                               f"Selected recordId column {component.properties.destinationColumnNames} is not present in input schema.",
                               SeverityLevelEnum.Error))

        if not component.properties.outputDistance:
            if not component.properties.outputCardDirection:
                if not component.properties.outputDirectionDegrees:
                    diagnostics.append(
                        Diagnostic("properties.outputDistance", f"Please select at least one output column option",
                                   SeverityLevelEnum.Error)
                    )

        return diagnostics

    def onChange(self, context: SqlContext, oldState: Component, newState: Component) -> Component:
        # Handle changes in the component's state and return the new state
        schema = json.loads(str(newState.ports.inputs[0].schema).replace("'", '"'))
        fields_array = [{"name": field["name"], "dataType": field["dataType"]["type"]} for field in schema["fields"]]
        relation_name = self.get_relation_names(newState, context)

        newProperties = dataclasses.replace(
            newState.properties,
            schema=json.dumps(fields_array),
            relation_name=relation_name
        )
        return newState.bindProperties(newProperties)

    def apply(self, props: DistanceProperties) -> str:
        # generate the actual macro call given the component's state
        resolved_macro_name = f"{self.projectName}.{self.name}"
        arguments = [
            json.dumps(props.relation_name),
            props.schema if props.schema else "[]",
            self._sql_string_literal(props.sourceColumnNames),
            self._sql_string_literal(props.destinationColumnNames),
            self._sql_string_literal(props.sourceType),
            self._sql_string_literal(props.destinationType),
            str(props.outputDistance).lower(),
            self._sql_string_literal(props.units),
            str(props.outputCardDirection).lower(),
            str(props.outputDirectionDegrees).lower()
        ]
        params = ",".join([param for param in arguments])
        return f'{{{{ {resolved_macro_name}({params}) }}}}'

    # -------------------------------------------------------------------------
    # Property loading/unloading
    # -------------------------------------------------------------------------
    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        # load the component's state given default macro property representation
        parametersMap = self.convertToParameterMap(properties.parameters)
        return Distance.DistanceProperties(
            relation_name=self._parse_relation_name_list(parametersMap.get("relation_name")),
            schema=self._parse_schema_value(parametersMap.get("schema"), parametersMap.get("allColumnNames")),
            sourceColumnNames=self._strip_sql_string_literal(parametersMap.get('sourceColumnNames')),
            destinationColumnNames=self._strip_sql_string_literal(parametersMap.get('destinationColumnNames')),
            sourceType=self._strip_sql_string_literal(parametersMap.get('sourceType')),
            destinationType=self._strip_sql_string_literal(parametersMap.get('destinationType')),
            outputDistance=parametersMap.get("outputDistance").lower() == "true",
            units=self._strip_sql_string_literal(parametersMap.get('units')),
            outputCardDirection=parametersMap.get("outputCardDirection").lower() == "true",
            outputDirectionDegrees=parametersMap.get("outputDirectionDegrees").lower() == "true",
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        # convert component's state to default macro property representation
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation_name", json.dumps(properties.relation_name)),
                MacroParameter("schema", properties.schema if properties.schema else "[]"),
                MacroParameter("sourceColumnNames", self._sql_string_literal(properties.sourceColumnNames)),
                MacroParameter("destinationColumnNames", self._sql_string_literal(properties.destinationColumnNames)),
                MacroParameter("sourceType", self._sql_string_literal(properties.sourceType)),
                MacroParameter("destinationType", self._sql_string_literal(properties.destinationType)),
                MacroParameter("outputDistance", str(properties.outputDistance).lower()),
                MacroParameter("units", self._sql_string_literal(properties.units)),
                MacroParameter("outputCardDirection", str(properties.outputCardDirection).lower()),
                MacroParameter("outputDirectionDegrees", str(properties.outputDirectionDegrees).lower()),
            ],
        )

    def updateInputPortSlug(self, component: Component, context: SqlContext):
        # Handle changes in the component's state and return the new state
        schema = json.loads(str(component.ports.inputs[0].schema).replace("'", '"'))
        fields_array = [{"name": field["name"], "dataType": field["dataType"]["type"]} for field in schema["fields"]]
        relation_name = self.get_relation_names(component, context)

        newProperties = dataclasses.replace(
            component.properties,
            schema=json.dumps(fields_array),
            relation_name=relation_name
        )
        return component.bindProperties(newProperties)
