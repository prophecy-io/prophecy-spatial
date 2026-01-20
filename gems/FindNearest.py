from dataclasses import dataclass
import dataclasses
import json

from collections import defaultdict
from prophecy.cb.sql.Component import *
from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *


class FindNearest(MacroSpec):
    name: str = "FindNearest"
    projectName: str = "prophecy_spatial"
    category: str = "Spatial"
    minNumOfInputPorts: int = 2
    supportedProviderTypes: list[ProviderTypeEnum] = [
        ProviderTypeEnum.Databricks,
        # ProviderTypeEnum.Snowflake,
        # ProviderTypeEnum.BigQuery,
        # ProviderTypeEnum.ProphecyManaged
    ]

    @dataclass(frozen=True)
    class FindNearestProperties(MacroProperties):
        # properties for the component with default values
        relation_name: List[str] = field(default_factory=list)
        source_schema: str = ''
        target_schema: str = ''
        columnNames: List[str] = field(default_factory=list)
        sourceColumnName: str = ""
        destinationColumnName: str = ""
        sourceType: str = "point"
        targetType: str = "point"
        nearestPoints: int = 1
        maxDistance: int = 20
        units: str = "kms"
        ignoreZeroDistance: bool = False

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

    def dialog(self) -> Dialog:
        horizontalDivider = HorizontalDivider()
        renameMethod = SelectBox("") \
            .addOption("Edit prefix/suffix", "editPrefixSuffix") \
            .addOption("Advanced rename", "advancedRename") \
            .bindProperty("renameMethod")

        dialog = Dialog("FindNearest") \
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
                                    SelectBox("Source Centroid Type").addOption("Point", "point").bindProperty("sourceType")
                                )
                                    .addColumn(
                                    SchemaColumnsDropdown("Source Centroid Column")
                                        .bindSchema("component.ports.inputs[0].schema")
                                        .bindProperty("sourceColumnName")
                                )
                                    .addColumn(
                                    SelectBox("Target Centroid Type").addOption("Point", "point").bindProperty("targetType")
                                )
                                    .addColumn(
                                    SchemaColumnsDropdown("Target Centroid Column")
                                        .bindSchema("component.ports.inputs[1].schema")
                                        .bindProperty("destinationColumnName")
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
                                .addElement(
                                    ColumnsLayout(gap="1rem", height="100%")
                                    .addColumn(
                                        NumberBox("How many nearest points to find?",
                                                placeholder="1",
                                                minValueVar=1
                                                )
                                        .bindProperty("nearestPoints")
                                    )
                                    .addColumn()
                                    .addColumn()
                                    .addColumn()
                                    .addColumn()
                                )
                                .addElement(NativeText("Maximum Distance"))
                                .addElement(
                                    ColumnsLayout(gap="1rem", height="100%")
                                    .addColumn(
                                                NumberBox("",
                                                        placeholder="20",
                                                        minValueVar=0
                                                        )
                                                .bindProperty("maxDistance")
                                    )
                                    .addColumn(
                                                SelectBox("").addOption("Kilometers", "kms").addOption("Miles","mls").addOption("Feet", "feet").addOption("Meters", "mtr").bindProperty("units")
                                    )
                                    .addColumn()
                                    .addColumn()
                                    .addColumn()
                            )
                            .addElement(Checkbox("Ignore 0 Distance Matches").bindProperty("ignoreZeroDistance"))
                        )
                    )
                )
                    .addElement(
                    AlertBox(
                        variant="success",
                        _children=[
                            Markdown(
                                "This gem requires that the Source Column and Destination Column contain geometric values in Well-Known Text (WKT) format. To convert longitude and latitude coordinates into WKT format, use the [CreatePoint gem](https://docs.prophecy.io/analysts/create-point/).\n\n"
                                "Example: If your table has columns like `source_longitude`, `source_latitude`, `target_longitude`, and `target_latitude`, first use the CreatePoint Gem to generate `source_geopoint` and `target_geopoint` columns in WKT format."
                            )
                        ]
                    )
                )
            )
        )
        return dialog

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        # Validate the component's state
        diagnostics = super(FindNearest, self).validate(context, component)

        if len(component.properties.sourceColumnName) == 0:
            diagnostics.append(
                Diagnostic("component.properties.sourceColumnName", f"Please select a source column",
                           SeverityLevelEnum.Error)
            )

        if len(component.properties.destinationColumnName) == 0:
            diagnostics.append(
                Diagnostic("component.properties.destinationColumnName", f"Please select a destination column",
                           SeverityLevelEnum.Error)
            )

        # Extract all column names from the schema
        source_field_names = [field["name"] for field in component.ports.inputs[0].schema["fields"]]
        target_field_names = [field["name"] for field in component.ports.inputs[1].schema["fields"]]

        if len(component.properties.sourceColumnName) > 0:
            if component.properties.sourceColumnName not in source_field_names:
                diagnostics.append(
                    Diagnostic("component.properties.sourceColumnName",
                               f"Selected column {component.properties.sourceColumnName} is not present in input schema.",
                               SeverityLevelEnum.Error))

        if len(component.properties.destinationColumnName) > 0:
            if component.properties.destinationColumnName not in target_field_names:
                diagnostics.append(
                    Diagnostic("component.properties.destinationColumnName",
                               f"Selected column {component.properties.destinationColumnName} is not present in input schema.",
                               SeverityLevelEnum.Error))

        return diagnostics

    def onChange(self, context: SqlContext, oldState: Component, newState: Component) -> Component:
        # Handle changes in the component's state and return the new state
        source_schema = json.loads(str(newState.ports.inputs[0].schema).replace("'", '"'))
        target_schema = json.loads(str(newState.ports.inputs[1].schema).replace("'", '"'))

        source_fields_array = [{"name": field["name"], "dataType": field["dataType"]["type"]} for field in source_schema["fields"]]
        target_fields_array = [{"name": field["name"], "dataType": field["dataType"]["type"]} for field in target_schema["fields"]]

        relation_name = self.get_relation_names(newState, context)

        newProperties = dataclasses.replace(
            newState.properties,
            source_schema=json.dumps(source_fields_array),
            target_schema=json.dumps(target_fields_array),
            relation_name=relation_name
        )
        return newState.bindProperties(newProperties)

    def apply(self, props: FindNearestProperties) -> str:
        # Get existing column names
        allSourceColumnNames = [field["name"] for field in json.loads(props.source_schema)]
        allTargetColumnNames = [field["name"] for field in json.loads(props.target_schema)]

        # generate the actual macro call given the component's state
        resolved_macro_name = f"{self.projectName}.{self.name}"
        arguments = [
            str(props.relation_name),
            "'" + props.sourceColumnName + "'",
            "'" + props.destinationColumnName + "'",
            "'" + str(props.sourceType) + "'",
            "'" + str(props.targetType) + "'",
            str(props.nearestPoints),
            str(props.maxDistance),
            "'" + str(props.units) + "'",
            str(props.ignoreZeroDistance).lower(),
            str(allSourceColumnNames),
            str(allTargetColumnNames)
        ]
        params = ",".join([param for param in arguments])
        return f'{{{{ {resolved_macro_name}({params}) }}}}'

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:

        # load the component's state given default macro property representation
        parametersMap = self.convertToParameterMap(properties.parameters)
        return FindNearest.FindNearestProperties(
            relation_name=parametersMap.get('relation_name'),
            source_schema=parametersMap.get('source_schema'),
            target_schema=parametersMap.get('target_schema'),
            sourceColumnName=parametersMap.get('sourceColumnName'),
            destinationColumnName=parametersMap.get('destinationColumnName'),
            sourceType=parametersMap.get('sourceType'),
            targetType=parametersMap.get('targetType'),
            nearestPoints=float(parametersMap.get('nearestPoints')),
            maxDistance=float(parametersMap.get('maxDistance')),
            units=parametersMap.get('units'),
            ignoreZeroDistance=parametersMap.get('ignoreZeroDistance').lower() == 'true'
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        # convert component's state to default macro property representation
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation_name", str(properties.relation_name)),
                MacroParameter("source_schema", str(properties.source_schema)),
                MacroParameter("target_schema", str(properties.target_schema)),
                MacroParameter("sourceColumnName", properties.sourceColumnName),
                MacroParameter("destinationColumnName", properties.destinationColumnName),
                MacroParameter("sourceType", properties.sourceType),
                MacroParameter("targetType", properties.targetType),
                MacroParameter("nearestPoints", str(properties.nearestPoints)),
                MacroParameter("maxDistance", str(properties.maxDistance)),
                MacroParameter("units", properties.units),
                MacroParameter("ignoreZeroDistance", str(properties.ignoreZeroDistance).lower())
            ],
        )

    def updateInputPortSlug(self, component: Component, context: SqlContext):
        # Handle changes in the component's state and return the new state
        source_schema = json.loads(str(component.ports.inputs[0].schema).replace("'", '"'))
        target_schema = json.loads(str(component.ports.inputs[1].schema).replace("'", '"'))

        source_fields_array = [{"name": field["name"], "dataType": field["dataType"]["type"]} for field in source_schema["fields"]]
        target_fields_array = [{"name": field["name"], "dataType": field["dataType"]["type"]} for field in target_schema["fields"]]

        relation_name = self.get_relation_names(component, context)

        newProperties = dataclasses.replace(
            component.properties,
            source_schema=json.dumps(source_fields_array),
            target_schema=json.dumps(target_fields_array),
            relation_name=relation_name
        )
        return component.bindProperties(newProperties)