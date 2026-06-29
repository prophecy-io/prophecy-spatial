import dataclasses
import json
import re
from dataclasses import dataclass, field

from prophecy.cb.sql.MacroBuilderBase import *
from prophecy.cb.ui.uispec import *

class Simplify(MacroSpec):
    name: str = "Simplify"
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
    class SimplifyProperties(MacroProperties):
        # properties for the component with default values
        relation_name: List[str] = field(default_factory=list)
        schema: str = ""
        tolerance: str = "1"
        unit: str = "kms"
        geom_column_name: str = ""

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
        return Dialog("Simplify").addElement(
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
                    SchemaColumnsDropdown("Geometry column (WKT format)")
                        .bindSchema("component.ports.inputs[0].schema")
                        .bindProperty("geom_column_name")
                )                               
                .addElement(
                    TextBox("Tolerance", placeholder="1.0").bindProperty("tolerance")
                )                
                .addElement(
                    SelectBox("Units").addOption("Miles", "miles").addOption("Kilometers", "kms").bindProperty("unit")
                )                                
           )
       )

    def validate(self, context: SqlContext, component: Component) -> List[Diagnostic]:
        diagnostics = []
        if len(component.properties.tolerance.strip()) == 0:
            diagnostics.append(
                Diagnostic(
                    "properties.tolerance",
                    "Field 'Tolerance' cannot be empty.",
                    SeverityLevelEnum.Error
                )   
            )
        else:
            try:
                float(component.properties.tolerance)
            except ValueError as e:
                diagnostics.append(
                    Diagnostic(
                        "properties.tolerance",
                        "Field 'Tolerance' must be a float.",
                        SeverityLevelEnum.Error
                    )
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

    def apply(self, props: SimplifyProperties) -> str:
        # generate the actual macro call given the component's
        resolved_macro_name = f"{self.projectName}.{self.name}"

        arguments = [
            str(props.relation_name),
            props.schema,
            "'" + props.geom_column_name + "'",            
            str(props.tolerance),
            "'" + props.unit + "'"
        ]

        params = ",".join([param for param in arguments])
        return f'{{{{ {resolved_macro_name}({params}) }}}}'

    # -------------------------------------------------------------------------
    # Property loading/unloading
    # -------------------------------------------------------------------------
    def loadProperties(self, properties: MacroProperties) -> PropertiesType:
        # load the component's state given default macro property representation
        parametersMap = self.convertToParameterMap(properties.parameters)
        return Simplify.SimplifyProperties(
            relation_name=json.loads(parametersMap.get('relation_name').replace("'", '"')),
            schema=parametersMap.get('schema').lstrip("'").rstrip("'"),
            geom_column_name=parametersMap.get('geom_column_name').lstrip("'").rstrip("'"),
            tolerance=str(parametersMap.get("tolerance")),
            unit=parametersMap.get('unit').lstrip("'").rstrip("'"),
        )

    def unloadProperties(self, properties: PropertiesType) -> MacroProperties:
        # convert component's state to default macro property representation
        return BasicMacroProperties(
            macroName=self.name,
            projectName=self.projectName,
            parameters=[
                MacroParameter("relation_name", json.dumps(properties.relation_name)),
                MacroParameter("schema", str(properties.schema)),
                MacroParameter("geom_column_name", str(properties.geom_column_name)),
                MacroParameter("tolerance", str(properties.tolerance)),
                MacroParameter("unit", str(properties.unit)),
            ],
        )

    def updateInputPortSlug(self, component: Component, context: SqlContext):
        schema = (json.loads(component.ports.inputs[0].schema) if isinstance(component.ports.inputs[0].schema, str) else (component.ports.inputs[0].schema or {}))
        fields_array = [{"name": field["name"], "dataType": field["dataType"]["type"]} for field in schema["fields"]]
        relation_name = self.get_relation_names(component, context)

        newProperties = dataclasses.replace(
            component.properties,
            schema=json.dumps(fields_array),
            relation_name=relation_name
        )
        return component.bindProperties(newProperties)
