import re
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
        relation_name = []
        for input_port in component.ports.inputs:
            if input_port.slug and not re.match(r'^in\d+$', input_port.slug):
                relation_name.append(input_port.slug)
            else:
                upstream_label = ""
                for connection in context.graph.connections:
                    if connection.targetPort == input_port.id:
                        upstream_node = context.graph.nodes.get(connection.source)
                        if upstream_node is not None and upstream_node.label is not None:
                            upstream_label = upstream_node.label
                relation_name.append(upstream_label)
        return relation_name

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

        schema = (json.loads(component.ports.inputs[0].schema) if isinstance(component.ports.inputs[0].schema, str) else (component.ports.inputs[0].schema or {}))
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
        schema = (json.loads(newState.ports.inputs[0].schema) if isinstance(newState.ports.inputs[0].schema, str) else (newState.ports.inputs[0].schema or {}))
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
            str(props.relation_name),
            props.schema,
            f"'{props.sourceColumnNames}'",
            f"'{props.destinationColumnNames}'",
            f"'{props.sourceType}'",
            f"'{props.destinationType}'",
            str(props.outputDistance).lower(),
            f"'{props.units}'",
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
            relation_name=json.loads(parametersMap.get('relation_name').replace("'", '"')),
            schema=parametersMap.get('schema'),
            sourceColumnNames=parametersMap.get('sourceColumnNames').lstrip("'").rstrip("'"),
            destinationColumnNames=parametersMap.get('destinationColumnNames').lstrip("'").rstrip("'"),
            sourceType=parametersMap.get('sourceType').lstrip("'").rstrip("'"),
            destinationType=parametersMap.get('destinationType').lstrip("'").rstrip("'"),
            outputDistance=parametersMap.get("outputDistance").lower() == "true",
            units=parametersMap.get('units').lstrip("'").rstrip("'"),
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
                MacroParameter("schema", str(properties.schema)),
                MacroParameter("sourceColumnNames", properties.sourceColumnNames),
                MacroParameter("destinationColumnNames", properties.destinationColumnNames),
                MacroParameter("sourceType", properties.sourceType),
                MacroParameter("destinationType", properties.destinationType),
                MacroParameter("outputDistance", str(properties.outputDistance).lower()),
                MacroParameter("units", properties.units),
                MacroParameter("outputCardDirection", str(properties.outputCardDirection).lower()),
                MacroParameter("outputDirectionDegrees", str(properties.outputDirectionDegrees).lower()),
            ],
        )

    def updateInputPortSlug(self, component: Component, context: SqlContext):
        # Handle changes in the component's state and return the new state
        schema = (json.loads(component.ports.inputs[0].schema) if isinstance(component.ports.inputs[0].schema, str) else (component.ports.inputs[0].schema or {}))
        fields_array = [{"name": field["name"], "dataType": field["dataType"]["type"]} for field in schema["fields"]]
        relation_name = self.get_relation_names(component, context)

        newProperties = dataclasses.replace(
            component.properties,
            schema=json.dumps(fields_array),
            relation_name=relation_name
        )
        return component.bindProperties(newProperties)
