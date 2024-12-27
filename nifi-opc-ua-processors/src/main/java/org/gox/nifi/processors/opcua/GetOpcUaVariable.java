package org.gox.nifi.processors.opcua;

import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.nodes.UaVariableNode;
import org.eclipse.milo.opcua.stack.core.UaException;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class GetOpcUaVariable extends AbstractProcessor {

    private OpcUaClient client;

    public static final PropertyDescriptor OPC_UA_SERVER_URL = new PropertyDescriptor
            .Builder().name("opc-ua-server-url")
            .displayName("OPC UA Server url")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor OPC_UA_VARIABLE_NAMESPACE = new PropertyDescriptor
            .Builder().name("opc-ua-variable-namespace")
            .displayName("OPC UA Variable Namespace")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor OPC_UA_VARIABLE_ID = new PropertyDescriptor
            .Builder().name("opc-ua-variable-id")
            .displayName("OPC UA Variable ID")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship MY_RELATIONSHIP = new Relationship.Builder()
            .name("MY_RELATIONSHIP")
            .description("Example relationship")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = new ArrayList<>();
        descriptors.add(OPC_UA_SERVER_URL);
        descriptors.add(OPC_UA_VARIABLE_NAMESPACE);
        descriptors.add(OPC_UA_VARIABLE_ID);
        descriptors = Collections.unmodifiableList(descriptors);

        relationships = new HashSet<>();
        relationships.add(MY_RELATIONSHIP);
        relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws UaException {
        try {
            client = OpcUaClient.create(context.getProperty(OPC_UA_SERVER_URL).getValue());
            client.connect().get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        int namespace = Integer.parseInt(context.getProperty(OPC_UA_VARIABLE_NAMESPACE).getValue());
        String id = context.getProperty(OPC_UA_VARIABLE_ID).getValue();
        try {
            UaVariableNode node = client.getAddressSpace().getVariableNode(
                new NodeId(namespace, id)
            );

            DataValue value = node.readValue();;
            FlowFile flowFile = session.create();
            session.putAttribute(flowFile, "OPC_UA", Boolean.toString(true));
            flowFile = session.write(flowFile, out -> out.write(value.getValue().getValue().toString().getBytes(StandardCharsets.UTF_8)));
            session.transfer(flowFile, MY_RELATIONSHIP);
        } catch (UaException e) {
            throw new RuntimeException(e);
        }
    }
}
