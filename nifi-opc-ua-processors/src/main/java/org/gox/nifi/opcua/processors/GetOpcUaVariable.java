package org.gox.nifi.opcua.processors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
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
import org.eclipse.milo.opcua.sdk.core.nodes.VariableNode;
import org.eclipse.milo.opcua.stack.core.UaException;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.structured.ReferenceDescription;
import org.gox.nifi.opcua.processors.model.OpcUaNode;
import org.gox.nifi.opcua.processors.util.DateUtils;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({"opcua","milo","browsing","variable"})
@CapabilityDescription("Custom processor NiFi pour lire des variables OPC UA, avec possibilité de browsing récursif.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class GetOpcUaVariable extends AbstractProcessor {

    private OpcUaClient client;
    private ObjectMapper objectMapper = new ObjectMapper();

    // 1) Propriétés classiques : URL du serveur
    public static final PropertyDescriptor OPC_UA_SERVER_URL = new PropertyDescriptor
            .Builder().name("opc-ua-server-url")
            .displayName("OPC UA Server URL")
            .description("Endpoint du serveur OPC UA (ex: opc.tcp://localhost:4334)")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    // 2) Propriété : mode de browsing (true/false)
    public static final PropertyDescriptor OPC_UA_BROWSING = new PropertyDescriptor
            .Builder().name("opc-ua-browsing")
            .displayName("Browsing Mode")
            .description("Si 'true', on browse récursivement. Si 'false', on lit les nodeIds spécifiés.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    // 3) Propriétés pour le browsing
    //    NodeId racine pour lancer le browse (par défaut, "ObjectsFolder" => ns=0;i=85)
    public static final PropertyDescriptor OPC_UA_BROWSING_ROOT_NODEID = new PropertyDescriptor
            .Builder().name("opc-ua-browsing-root-nodeid")
            .displayName("Browsing Root NodeId")
            .description("Le NodeId de départ pour le browsing récursif (ex: ns=0;i=85 pour ObjectsFolder).")
            .defaultValue("ns=0;i=85")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    // 4) Propriétés pour la lecture statique d’une liste de NodeIds
    public static final PropertyDescriptor OPC_UA_NODEID_LIST = new PropertyDescriptor
            .Builder().name("opc-ua-nodeid-list")
            .displayName("OPC UA NodeId List")
            .description("Liste de NodeIds à lire, séparés par des virgules (ex: ns=1;s=MyVar,ns=1;s=AnotherVar)")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    // 5) Relation de sortie
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Flux de sortie principal")
            .build();

    // Liste finale des PropertyDescriptors
    private List<PropertyDescriptor> descriptors;

    // Liste finale des Relationships
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        objectMapper.registerModule(new JavaTimeModule());

        List<PropertyDescriptor> descs = new ArrayList<>();
        descs.add(OPC_UA_SERVER_URL);
        descs.add(OPC_UA_BROWSING);
        descs.add(OPC_UA_BROWSING_ROOT_NODEID);
        descs.add(OPC_UA_NODEID_LIST);
        descriptors = Collections.unmodifiableList(descs);

        Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        relationships = Collections.unmodifiableSet(rels);
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
            String url = context.getProperty(OPC_UA_SERVER_URL).getValue();
            client = OpcUaClient.create(url);
            client.connect().get();
        } catch (Exception e) {
            throw new RuntimeException("Impossible de se connecter au serveur OPC UA", e);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        boolean browsing = Boolean.parseBoolean(context.getProperty(OPC_UA_BROWSING).getValue());

        if (browsing) {
            // MODE BROWSING RECURSIF
            String rootNodeIdString = context.getProperty(OPC_UA_BROWSING_ROOT_NODEID).getValue();
            NodeId rootNodeId = parseNodeIdString(rootNodeIdString);
            if (rootNodeId == null) {
                getLogger().error("NodeId racine invalide pour le browsing : " + rootNodeIdString);
                return;
            }

            try {
                // 1) Récupère la liste TOTALE des NodeIds de type "VariableNode" (découverts récursivement)
                List<NodeId> variableNodeIds = new ArrayList<>();
                browseRecursivelyAndCollectVariables(rootNodeId, variableNodeIds);

                // 2) Lit toutes les variables trouvées
                for (NodeId variableNodeId : variableNodeIds) {
                    readAndCreateFlowFile(variableNodeId, session);
                }

            } catch (Exception e) {
                getLogger().error("Erreur lors du browsing ou de la lecture OPC UA : ", e);
            }

        } else {
            // MODE LECTURE D'UNE LISTE DE NODEIDs
            String nodeIdList = context.getProperty(OPC_UA_NODEID_LIST).getValue();
            if (nodeIdList == null || nodeIdList.trim().isEmpty()) {
                getLogger().warn("Aucun NodeId spécifié, rien à lire.");
                return;
            }

            // ex: "ns=1;s=MyVar,ns=1;s=AnotherVar"
            String[] nodeIdsArray = nodeIdList.split(",");
            for (String nodeIdStr : nodeIdsArray) {
                nodeIdStr = nodeIdStr.trim();
                NodeId nodeId = parseNodeIdString(nodeIdStr);
                if (nodeId == null) {
                    getLogger().warn("NodeId invalide : " + nodeIdStr);
                    continue;
                }
                try {
                    readAndCreateFlowFile(nodeId, session);
                } catch (Exception e) {
                    getLogger().error("Erreur lors de la lecture du NodeId: " + nodeIdStr, e);
                }
            }
        }
    }

    /**
     * Parcours récursif de l'arborescence OPC UA depuis un nodeId racine,
     * et récupération de tous les NodeIds de type "VariableNode" (UaVariableNode).
     */
    private void browseRecursivelyAndCollectVariables(NodeId nodeId, List<NodeId> variableNodeIds) throws Exception {
        // On browse les références enfants
        List<ReferenceDescription> references = client.getAddressSpace().browse(nodeId);

        for (ReferenceDescription ref : references) {
            // Résoudre le NodeId local (car ref.getNodeId() peut être élargi ou externe)
            NodeId childNodeId = ref.getNodeId().toNodeId(client.getNamespaceTable()).orElse(null);
            if (childNodeId == null) {
                continue;
            }

            // Vérifier si c'est une variable
            // On peut faire un simple test : tenter de récupérer comme VariableNode
            try {
                VariableNode varNode = client.getAddressSpace().getVariableNode(childNodeId);
                if (varNode != null) {
                    // C'est une variable
                    variableNodeIds.add(childNodeId);
                }
            } catch (UaException ignore) {
                // pas une variable
            }

            // Parcours récursif (car on peut avoir des dossiers ou objets enfants)
            browseRecursivelyAndCollectVariables(childNodeId, variableNodeIds);
        }
    }

    /**
     * Lit la valeur d'un NodeId (de type variable) et crée un FlowFile dans NiFi.
     */
    private void readAndCreateFlowFile(NodeId variableNodeId, ProcessSession session) throws Exception {
        UaVariableNode varNode = client.getAddressSpace().getVariableNode(variableNodeId);
        if (varNode == null) {
            getLogger().warn("Impossible de récupérer le node en tant que VariableNode: " + variableNodeId);
            return;
        }

        DataValue value = varNode.readValue();
        // Créer le flowFile
        FlowFile flowFile = session.create();
        // Ajouter éventuellement des attributs
        flowFile = session.putAttribute(flowFile, "opcua.nodeId", variableNodeId.toParseableString());
        flowFile = session.putAttribute(flowFile, "opcua.status", value.getStatusCode().toString());
        flowFile = session.putAttribute(flowFile, "mime.type", "application/json");

        // Écrire la valeur dans le content
        Object val = value.getValue().getValue();
        String valStr = (val != null) ? val.toString() : "null";

        OpcUaNode opcUaNode = new OpcUaNode(varNode.getNodeId().getIdentifier().toString(),
                varNode.getBrowseName().getName(),
                varNode.getDataType().getType().toString(),
                valStr,
                value.getSourceTime() != null ? DateUtils.convert(value.getSourceTime().getJavaDate()) : LocalDateTime.now());
        getLogger().info("JSON OPC UA node", opcUaNode.toString());

        String opcUaNodeJsonStr = objectMapper.writeValueAsString(opcUaNode);
        flowFile = session.write(flowFile, out -> out.write(opcUaNodeJsonStr.getBytes(StandardCharsets.UTF_8)));

        // Transférer
        session.transfer(flowFile, REL_SUCCESS);
        getLogger().info("Lu variable={} => {}", variableNodeId, valStr);
    }

    /**
     * Convertit une chaîne de type "ns=1;i=1234" ou "ns=2;s=MyVar" en NodeId.
     */
    private NodeId parseNodeIdString(String nodeIdStr) {
        // Très simpliste : on cherche "ns=X; s=..." ou "ns=X; i=..."
        // Dans la vraie vie, vous pourriez utiliser NodeId.parse() s’il est dispo,
        // ou faire un parsing plus robuste.
        try {
            return NodeId.parse(nodeIdStr);
        } catch (Throwable t) {
            getLogger().error("Erreur parsing NodeId: {}", new Object[]{nodeIdStr}, t);
            return null;
        }
    }
}
