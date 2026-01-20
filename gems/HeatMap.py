import dataclasses
from dataclasses import dataclass

from prophecy.cb.sql.Component import *
from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *


class HeatMap(MacroSpec):
    name: str = "HeatMap"
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
    class HeatMapProperties(MacroProperties):
        # properties for the component with default values
        relation_name: List[str] = field(default_factory=list)
        longitudeColumnName: str = ""
        latitudeColumnName: str = ""
        heatColumnName: str = ""
        decayType: str = "constant"
        resolution: int = 8
        gridDistance: int = 1

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
        dialog = Dialog("HeatMap") \
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
                                TitleElement("Choose Geo Points")
                            )
                            .addElement(
                                ColumnsLayout(gap="1rem", height="100%")
                                .addColumn(
                                    SchemaColumnsDropdown("Longitude Column Name")
                                    .bindSchema("component.ports.inputs[0].schema")
                                    .bindProperty("longitudeColumnName")
                                )
                                .addColumn(
                                    SchemaColumnsDropdown("Latitude Column Name")
                                    .bindSchema("component.ports.inputs[0].schema")
                                    .bindProperty("latitudeColumnName")
                                )
                                .addColumn()
                                .addColumn()
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
                            .addElement(
                                TitleElement("Advanced Settings")
                            )
                            .addElement(
                                ColumnsLayout(gap="1rem", height="100%")
                                .addColumn(
                                    SchemaColumnsDropdown("Heat Column Name")
                                    .bindSchema("component.ports.inputs[0].schema")
                                    .bindProperty("heatColumnName")
                                )
                                .addColumn(
                                    SelectBox("Decay Function")
                                    .addOption("Constant", "constant")
                                    .addOption("Linear", "linear")
                                    .addOption("Exponential", "exp")
                                    .bindProperty("decayType")
                                )
                                .addColumn()
                                .addColumn()
                            )
                            .addElement(
                                ColumnsLayout(gap="1rem", height="100%")
                                .addColumn(
                                    NumberBox("Resolution", placeholder="0", minValueVar=0, maxValueVar=15)
                                    .bindProperty("resolution")
                                )
                                .addColumn(
                                    NumberBox("Grid Distance", placeholder="0", minValueVar=0)
                                    .bindProperty("gridDistance")
                                )
                                .addColumn()
                                .addColumn()
                            )
                        )
                    )
                )
                .addElement(
                    AlertBox(
                        variant="success",
                        _children=[
                            Markdown(
                                "**Advanced Settings**"
                                "\n"
                                "- **Resolution**: H3 resolution controls how big each hexagon is — lower resolutions mean bigger hexes (like countries), higher resolutions mean smaller hexes (like street, buildings etc)"
                                "\n"
                                "- **Grid Distance**: Defines the number of hexagon steps away from the center to generate surronding hexagons"
                                "\n"
                                "- **Decay Function**: Determines how heat fades with distance: constant applies equal weight to all neighbors, linear reduces weight linearly with distance, and exponential halves the weight with each step away"
                            )
                        ]
                    )
                )
            )
        )
        return dialog

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        # Validate the component's state
        diagnostics = super(HeatMap, self).validate(context, component)

        if component.properties.longitudeColumnName is None or component.properties.longitudeColumnName == '':
            diagnostics.append(
                Diagnostic("component.properties.longitudeColumnName", "Please select the longitude column",
                           SeverityLevelEnum.Error))

        if component.properties.latitudeColumnName is None or component.properties.latitudeColumnName == '':
            diagnostics.append(
                Diagnostic("component.properties.latitudeColumnName", "Please select the latitude column",
                           SeverityLevelEnum.Error))

        # Extract all column names from the schema
        field_names = [field["name"] for field in component.ports.inputs[0].schema["fields"]]
        if component.properties.longitudeColumnName != '' and component.properties.longitudeColumnName not in field_names:
            diagnostics.append(
                Diagnostic("component.properties.longitudeColumnName",
                           f"Selected longitude column {component.properties.longitudeColumnName} is not present in input schema.",
                           SeverityLevelEnum.Error)
            )

        if component.properties.latitudeColumnName != '' and component.properties.latitudeColumnName not in field_names:
            diagnostics.append(
                Diagnostic("component.properties.latitudeColumnName",
                           f"Selected latitude column {component.properties.latitudeColumnName} is not present in input schema.",
                           SeverityLevelEnum.Error)
            )

        if component.properties.heatColumnName != '' and component.properties.heatColumnName not in field_names:
            diagnostics.append(
                Diagnostic("component.properties.heatColumnName",
                           f"Selected latitude column {component.properties.heatColumnName} is not present in input schema.",
                           SeverityLevelEnum.Error)
            )

        # ── Numeric‐type check for heatColumnName ────────────────────────────
        if component.properties.heatColumnName != '':
            fields_dict = { field["name"]: field["dataType"]["type"] for field in component.ports.inputs[0].schema["fields"] }
            dtype = fields_dict.get(component.properties.heatColumnName).lower()
            numeric_types = {
                "tinyint", "smallint", "int", "integer",
                "bigint", "float", "double", "decimal", "numeric"
            }

            if dtype not in numeric_types:
                diagnostics.append(
                    Diagnostic(
                        "component.properties.heatColumnName",
                        f"Selected heat column is of type '{dtype}', which is not numeric.",
                        SeverityLevelEnum.Error
                    )
                )

        return diagnostics

    def onChange(self, context: SqlContext, oldState: Component, newState: Component) -> Component:
        # Handle changes in the component's state and return the new state
        relation_name = self.get_relation_names(newState, context)

        newProperties = dataclasses.replace(
            newState.properties,
            relation_name=relation_name
        )
        return newState.bindProperties(newProperties)

    def apply(self, props: HeatMapProperties) -> str:
        # generate the actual macro call given the component's state
        resolved_macro_name = f"{self.projectName}.{self.name}"

        # Get the Single Table Name
        table_name: str = ",".join(str(rel) for rel in props.relation_name)

        arguments = [
            "'" + table_name + "'",
            "'" + props.longitudeColumnName + "'",
            "'" + props.latitudeColumnName + "'",
            str(props.resolution),
            str(props.gridDistance),
            "'" + props.heatColumnName + "'",
            "'" + props.decayType + "'"
        ]
        params = ",".join([param for param in arguments])
        return f'{{{{ {resolved_macro_name}({params}) }}}}'

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        # load the component's state given default macro property representation
        parametersMap = self.convertToParameterMap(properties.parameters)
        return HeatMap.HeatMapProperties(
            relation_name=parametersMap.get('relation_name'),
            longitudeColumnName=parametersMap.get('longitudeColumnName'),
            latitudeColumnName=parametersMap.get('latitudeColumnName'),
            resolution=int(parametersMap.get('resolution')),
            gridDistance=int(parametersMap.get('gridDistance')),
            heatColumnName=parametersMap.get('heatColumnName'),
            decayType=parametersMap.get('decayType')
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        # convert component's state to default macro property representation
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation_name", str(properties.relation_name)),
                MacroParameter("longitudeColumnName", properties.longitudeColumnName),
                MacroParameter("latitudeColumnName", properties.latitudeColumnName),
                MacroParameter("resolution", str(properties.resolution)),
                MacroParameter("gridDistance", str(properties.gridDistance)),
                MacroParameter("heatColumnName", str(properties.heatColumnName)),
                MacroParameter("decayType", str(properties.decayType))
            ],
        )

    def updateInputPortSlug(self, component: Component, context: SqlContext):
        relation_name = self.get_relation_names(component, context)

        newProperties = dataclasses.replace(
            component.properties,
            relation_name=relation_name
        )
        return component.bindProperties(newProperties)
