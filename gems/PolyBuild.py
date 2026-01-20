from dataclasses import dataclass
import dataclasses
import json

from collections import defaultdict
from prophecy.cb.sql.Component import *
from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *


class PolyBuild(MacroSpec):
    name: str = "PolyBuild"
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
    class PolyBuildProperties(MacroProperties):
        # properties for the component with default values
        relation_name: List[str] = field(default_factory=list)
        buildMethod: str = "SequencePolygon"
        longitudeColumnName: str = ""
        latitudeColumnName: str = ""
        groupColumnName: str = ""
        sequenceColumnName: str = ""

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
        dialog = Dialog("PolyBuild") \
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
                                        ColumnsLayout(gap="1rem", height="100%")
                                        .addColumn(
                                            SelectBox("Build Method")
                                            .addOption("Sequence Polygon", "SequencePolygon")
                                            .addOption("Sequence Polyline", "SequencePolyline")
                                            .bindProperty("buildMethod")
                                        )
                                        .addColumn()
                                        .addColumn()
                                        .addColumn()
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
                                    .addElement(
                                        ColumnsLayout(gap="1rem", height="100%")
                                        .addColumn(
                                            SchemaColumnsDropdown("Group Field")
                                                .bindSchema("component.ports.inputs[0].schema")
                                                .bindProperty("groupColumnName")
                                        )
                                        .addColumn()
                                        .addColumn()
                                        .addColumn()
                                    )
                                    .addElement(
                                        ColumnsLayout(gap="1rem", height="100%")
                                        .addColumn(
                                            SchemaColumnsDropdown("Sequence Field")
                                                .bindSchema("component.ports.inputs[0].schema")
                                                .bindProperty("sequenceColumnName")
                                        )
                                        .addColumn()
                                        .addColumn()
                                        .addColumn()
                                    )
                        )
                    )

                )
            )
        )
        return dialog

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        # Validate the component's state
        diagnostics = super(PolyBuild, self).validate(context, component)

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
        if component.properties.longitudeColumnName !='' and component.properties.longitudeColumnName not in field_names:
            diagnostics.append(
                Diagnostic("component.properties.longitudeColumnName", f"Selected longitude column {component.properties.longitudeColumnName} is not present in input schema.", SeverityLevelEnum.Error)
            )

        if component.properties.latitudeColumnName!='' and component.properties.latitudeColumnName not in field_names:
            diagnostics.append(
                Diagnostic("component.properties.latitudeColumnName", f"Selected latitude column {component.properties.latitudeColumnName} is not present in input schema.", SeverityLevelEnum.Error)
            )

        if component.properties.groupColumnName != '':
            if component.properties.groupColumnName not in field_names:
                diagnostics.append(
                    Diagnostic("component.properties.groupColumnName", f"Selected group column {component.properties.groupColumnName} is not present in input schema.", SeverityLevelEnum.Error)
                )

        if component.properties.sequenceColumnName != '':
            if component.properties.sequenceColumnName not in field_names:
                diagnostics.append(
                    Diagnostic("component.properties.sequenceColumnName", f"Selected sequence column {component.properties.sequenceColumnName} is not present in input schema.", SeverityLevelEnum.Error)
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

    def apply(self, props: PolyBuildProperties) -> str:
        # generate the actual macro call given the component's state
        resolved_macro_name = f"{self.projectName}.{self.name}"

        # Get the Single Table Name
        table_name: str = ",".join(str(rel) for rel in props.relation_name)

        arguments = [
            "'" + table_name + "'",
            "'" + props.buildMethod + "'",
            "'" + props.longitudeColumnName + "'",
            "'" + props.latitudeColumnName + "'",
            "'" + props.groupColumnName + "'",
            "'" + props.sequenceColumnName + "'"
        ]
        params = ",".join([param for param in arguments])
        return f'{{{{ {resolved_macro_name}({params}) }}}}'

    def loadProperties(self, properties: MacroProperties) -> PropertiesType:

        # load the component's state given default macro property representation
        parametersMap = self.convertToParameterMap(properties.parameters)
        return PolyBuild.PolyBuildProperties(
            relation_name=parametersMap.get('relation_name'),
            buildMethod=parametersMap.get('buildMethod'),
            longitudeColumnName=parametersMap.get('longitudeColumnName'),
            latitudeColumnName=parametersMap.get('latitudeColumnName'),
            groupColumnName=parametersMap.get('groupColumnName'),
            sequenceColumnName=parametersMap.get('sequenceColumnName')
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        # convert component's state to default macro property representation
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation_name", str(properties.relation_name)),
                MacroParameter("buildMethod", properties.buildMethod),
                MacroParameter("longitudeColumnName", properties.longitudeColumnName),
                MacroParameter("latitudeColumnName", properties.latitudeColumnName),
                MacroParameter("groupColumnName", properties.groupColumnName),
                MacroParameter("sequenceColumnName", properties.sequenceColumnName)
            ],
        )

    def updateInputPortSlug(self, component: Component, context: SqlContext):
        relation_name = self.get_relation_names(component, context)

        newProperties = dataclasses.replace(
            component.properties,
            relation_name=relation_name
        )
        return component.bindProperties(newProperties)