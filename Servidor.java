import java.io.*;
import java.net.*;
import java.util.*;

import javax.xml.crypto.Data;

import com.google.gson.Gson;

public class Servidor {

    private String meuIP;
    private int minhaPorta;
    private String ipLider;
    private int portaLider;
    private boolean souLider;
    private List<String> ipServidores = new ArrayList<>();
    private List<Integer> portaServidores = new ArrayList<>();
    private Map<String, List<String>> bancoKV; // HashMap para armazenar chave -> [valor, timestamp]
    private static List<Mensagem> requisicoesPendentes = new ArrayList<>();
    private final Gson gson = new Gson();

    private final int QUANTIDADE_SERVIDORES = 3;
    private static final int LATENCIA = 10000; // 10 segundos

    public Servidor() {
        bancoKV = new HashMap<>();
    }

    // Método principal para iniciar o servidor
    public void iniciar() {
        Scanner scanner = new Scanner(System.in);

        System.out.println("=== Servidor ===");

        System.out.print("Meu IP: ");
        meuIP = scanner.nextLine().trim();
        System.out.print("Minha porta: ");
        minhaPorta = Integer.parseInt(scanner.nextLine().trim());

        System.out.print("IP do líder: ");
        ipLider = scanner.nextLine().trim();
        System.out.print("Porta do líder: ");
        portaLider = Integer.parseInt(scanner.nextLine().trim());

        souLider = meuIP.equals(ipLider) && minhaPorta == portaLider;

        // Se o servidor for o líder, preenche a lista de seguidores
        if (souLider) {
            preencherSeguidores();
        }

        // Inicia o Socket do servidor
        try (ServerSocket serverSocket = new ServerSocket(minhaPorta)) {
            System.out.println("Servidor pronto em " + meuIP + ":" + minhaPorta);

            // Aceita conexões de clientes
            while (true) {
                Socket socket = serverSocket.accept();

                // Cria uma nova thread para atender a conexão
                new Atendente(socket).start();
            }

        } catch (IOException e) {
            System.out.println("Erro ao iniciar servidor: " + e.getMessage());
        }
    }

    // Método para preencher a lista de seguidores do líder
    public void preencherSeguidores() {
        Scanner scanner = new Scanner(System.in);

        for (int i = 0; i < QUANTIDADE_SERVIDORES - 1; i++) {
            System.out.print("Informe IP do servidor " + (i + 1) + ": ");
            String ip = scanner.nextLine().trim();
            System.out.print("Informe a porta do servidor " + (i + 1) + ": ");
            int porta = Integer.parseInt(scanner.nextLine().trim());
            ipServidores.add(ip);
            portaServidores.add(porta);
        }
    }

    // Método para tratar conexões recebidas
    public void tratarConexao(Socket socket) {

        try (
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                DataOutputStream out = new DataOutputStream(socket.getOutputStream())) {
            // Lê a mensagem JSON enviada pelo cliente
            String json = in.readLine();
            Mensagem msg = gson.fromJson(json, Mensagem.class);

            // Se a mensagem for nula, encerra o método
            if (msg == null)
                return;

            // Verifica o tipo de requisição e trata conforme necessário
            switch (msg.getRequisicao()) {
                case "PUT":
                    if (souLider) {
                        tratarPutComoLider(msg);
                    } else {
                        encaminharParaLider(msg);
                    }
                    break;
                case "GET":
                    tratarGet(msg);
                    break;
                case "REPLICATION":
                    tratarReplicacao(msg, out);
                    break;
                default:
                    System.out.println("Requisição desconhecida: " + msg.getRequisicao());
                    break;
            }

        } catch (Exception e) {
            System.out.println("Erro na conexão: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // Método para tratar requisições PUT como líder
    public void tratarPutComoLider(Mensagem msg) {
        System.out.println("Cliente " + msg.getIp() + ":" + msg.getPorta() + " PUT key:" + msg.getChave() + " value:" + msg.getValor());

        try {

            Thread.sleep(LATENCIA); // Simula processamento

            // Adiciona o timestamp atual à mensagem PUT
            Mensagem msgComTimestamp = new Mensagem("PUT", msg.getChave(), msg.getValor(), System.currentTimeMillis(), msg.getIp(), msg.getPorta());

            // Adiciona a mensagem ao banco de dados local
            putBanco(msgComTimestamp);

            // Verifica pendências locais de GET antes de replicar
            verificarPendencias(msgComTimestamp);

            // Se a replicação for bem-sucedida entra envia PUT_OK
            if (replicarPut(msgComTimestamp)) {

                System.out.println("Enviando PUT_OK ao Cliente " + msgComTimestamp.getIp() + ":" + msgComTimestamp.getPorta()
                        + " da key:" + msgComTimestamp.getChave() + " ts:" + msgComTimestamp.getTimestamp());

                // Mensagem de resposta para o cliente
                Mensagem resposta = new Mensagem("PUT_OK", msgComTimestamp.getChave(), msgComTimestamp.getValor(),
                    msgComTimestamp.getTimestamp(), meuIP, minhaPorta);

                // Abre uma nova conexão com o cliente e envia a resposta
                try (
                        Socket socket = new Socket(msgComTimestamp.getIp(), msgComTimestamp.getPorta());
                        DataOutputStream outCliente = new DataOutputStream(socket.getOutputStream());) {

                    outCliente.writeBytes(gson.toJson(resposta) + "\n");
                
                // Caso ocorra um erro ao enviar a resposta 
                } catch (IOException e) {
                    System.out.println("Erro ao enviar PUT_OK para o cliente: " + e.getMessage() + "" +
                            "Cliente IP: " + msgComTimestamp.getIp() + ", Porta: " + msgComTimestamp.getPorta());
                }

            // Caso a replicação falhe
            } else {
                System.out.println("Erro ao replicar PUT para seguidores.");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    // Método para replicar o PUT para os seguidores. Se todos os seguidores responderem com sucesso, retorna true
    private boolean replicarPut(Mensagem msgComTimestamp) {
        int acks = 0;

        // Envia a replicação para todos os seguidores
        for (int i = 0; i < ipServidores.size(); i++) {
            String ipSeguidor = ipServidores.get(i);
            int portaSeguidor = portaServidores.get(i);

            try {
                // Simula um atraso
                Thread.sleep(LATENCIA);
                if (enviarReplicacao(msgComTimestamp, ipSeguidor, portaSeguidor)) {
                    acks++;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // Se a quantidade de confirmações for igual ao número de seguidores menos um (o líder não conta)
        return acks == QUANTIDADE_SERVIDORES - 1;
    }

    // Método para enviar a replicação para um servidor específico. Se a replicação for bem-sucedida, retorna true
    public boolean enviarReplicacao(Mensagem msg, String ip, int porta) {

        // Abre uma nova conexão com o servidor seguidor e envia a mensagem de replicação
        try (
                Socket socket = new Socket(ip, porta);
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
            out.writeBytes(gson.toJson(new Mensagem("REPLICATION", msg.getChave(), msg.getValor(), msg.getTimestamp())) + "\n");

            // Lê a resposta do servidor seguidor
            // Blocking read, espera até receber uma resposta -> pois para seguir precisa receber REPLICATION_OK de todos
            String respostaJson = in.readLine();
            Mensagem resposta = gson.fromJson(respostaJson, Mensagem.class);

            // Se a resposta for válida e indicar sucesso, retorna true
            if (resposta != null && "REPLICATION_OK".equals(resposta.getRequisicao())) {
                return true;
            }

        } catch (Exception e) {
            System.out.println("Erro replicando para " + ip + ":" + porta);
        }
        return false;
    }

    // Método para encaminhar a mensagem PUT para o líder
    public void encaminharParaLider(Mensagem msg) {

        // Abre uma nova conexão com o líder e envia a mensagem PUT
        try (
                Socket socket = new Socket(ipLider, portaLider);
                DataOutputStream outLider = new DataOutputStream(socket.getOutputStream());
                BufferedReader inLider = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            // Envia a mensagem PUT para o líder
            System.out.println("Encaminhando PUT key:" + msg.getChave() + " value:" + msg.getValor());
            outLider.writeBytes(gson.toJson(msg) + "\n");

        } catch (Exception e) {
            System.out.println("Erro ao encaminhar para o líder.");
        }
    }

    // Método para tratar requisições GET
    public void tratarGet(Mensagem msgCliente) throws IOException {

        // Obtém o dado do banco de dados local usando a chave da mensagem recebida
        Mensagem dado = getBanco(msgCliente.getChave());

        try (
                Socket socket = new Socket(msgCliente.getIp(), msgCliente.getPorta());
                DataOutputStream outCliente = new DataOutputStream(socket.getOutputStream());) {

            // Se a chave não existir, retorna null
            if (dado == null) {
                // Caso 1: dado é null e timestamp do cliente é zero (primeira requisição)
                if (msgCliente.getTimestamp() == 0L) {
                    outCliente.writeBytes(
                            gson.toJson(new Mensagem("GET", msgCliente.getChave(), msgCliente.getValor(), 0L, meuIP, minhaPorta)) + "\n");
                    return;
                }

                // Caso 2: dado é null, mas timestamp do cliente > 0
                outCliente.writeBytes(gson.toJson(new Mensagem("WAIT_FOR_RESPONSE")) + "\n");

                // Adiciona a requisição pendente
                // utiliza synchronized para evitar problemas de concorrência
                synchronized (requisicoesPendentes) {
                    requisicoesPendentes.add(msgCliente);
                }
                return;
            }

            // Se o timestamp da chave local for menor que o timestamp do cliente, ele espera pela resposta
            if (dado.getTimestamp() < msgCliente.getTimestamp()) {

                outCliente.writeBytes(gson.toJson(new Mensagem("WAIT_FOR_RESPONSE")) + "\n");

                // Adiciona a requisição pendente
                // utiliza synchronized para evitar problemas de concorrência
                synchronized (requisicoesPendentes) {
                    requisicoesPendentes.add(msgCliente);
                }

            } else {

                // Se a chave existir e o timestamp for maior ou igual, retorna o valor
                Mensagem resposta = new Mensagem("GET", dado.getChave(), dado.getValor(), dado.getTimestamp(), meuIP, minhaPorta);

                System.out.println("Cliente " + msgCliente.getIp() + ":" + msgCliente.getPorta() +
                        " GET key:" + msgCliente.getChave() + " ts:" + msgCliente.getTimestamp() +
                        ". Meu ts é " + dado.getTimestamp() + ", portanto devolvendo " + dado.getValor());

                outCliente.writeBytes(gson.toJson(resposta) + "\n");
            }
        } catch (IOException e) {
            System.out.println("Erro ao tratar GET: " + e.getMessage());
        }
    }

    // Método para tratar replicação de dados
    public void tratarReplicacao(Mensagem msg, DataOutputStream out) throws IOException {

        System.out.println("REPLICATION key:" + msg.getChave() + " value:" + msg.getValor() + " ts:" + msg.getTimestamp());

        // Faz a replicação no banco de dados local
        putBanco(msg);

        // Envia confirmação de replicação para o líder
        out.writeBytes(gson.toJson(new Mensagem("REPLICATION_OK")) + "\n");

        // Verifica pendências de GETs que podem ter sido solicitados antes da replicação
        verificarPendencias(msg);
    }

    // Método para verificar pendências de GETs que podem ter sido solicitados antes da replicação
    private synchronized void verificarPendencias(Mensagem mensagemCliente) {

        // Verifica se há requisições pendentes que precisam ser reenviadas
        Iterator<Mensagem> it = requisicoesPendentes.iterator();
        while (it.hasNext()) {
            Mensagem pendente = it.next();
            if (pendente.getChave().equals(mensagemCliente.getChave()) && mensagemCliente.getTimestamp() >= pendente.getTimestamp()) {

                // Reenvia o GET pendente para o cliente
                reenviarGetPendente(pendente, mensagemCliente);

                // Remove a requisição pendente da lista
                it.remove();
            }
        }
    }

    // Método para reenviar GET pendente para o cliente
    private void reenviarGetPendente(Mensagem mensagemPendente, Mensagem mensagemCliente) {

        // Abre uma nova conexão com o cliente onde a requisição pendente foi feita
        try (
                Socket socket = new Socket(mensagemPendente.getIp(), mensagemPendente.getPorta());
                DataOutputStream outCliente = new DataOutputStream(socket.getOutputStream());) {
                
                Mensagem resposta = new Mensagem("WAIT FOR RESPONSE : GET", mensagemCliente.getChave(), mensagemCliente.getValor(),
                    mensagemCliente.getTimestamp(), meuIP, minhaPorta);

            System.out.println("Cliente " + mensagemPendente.getIp() + ":" + mensagemPendente.getPorta() +
                    " GET key:" + mensagemPendente.getChave() + " ts:" + mensagemPendente.getTimestamp() +
                    ". Meu ts é " + mensagemCliente.getTimestamp() + ", portanto devolvendo " + mensagemCliente.getValor());
            
            // Envia a resposta para o cliente
            outCliente.writeBytes(gson.toJson(resposta) + "\n");

        } catch (IOException e) {
            System.out.println(
                    "Erro ao reenviar GET pendente para " + mensagemPendente.getIp() + ":" + mensagemPendente.getPorta());
        }
    }

    // Método para adicionar uma mensagem ao banco de dados local
    // Este método é sincronizado para evitar problemas de concorrência
    private void putBanco(Mensagem msg) {
        synchronized (bancoKV) {
            List<String> valueTimestamp = new ArrayList<>();

            valueTimestamp.add(String.valueOf(msg.getValor()));
            valueTimestamp.add(String.valueOf(msg.getTimestamp()));

            // Adiciona a chave e o valor com timestamp ao bancoKV
            bancoKV.put(msg.getChave(), valueTimestamp);
        }
    }

    // Método para obter uma mensagem do banco de dados local
    // Retorna uma Mensagem com a chave, valor e timestamp
    // Este método é sincronizado para evitar problemas de concorrência
    private Mensagem getBanco(String chave) {
        synchronized (bancoKV) {
            List<String> valueTimestamp = bancoKV.get(chave);

            if (valueTimestamp != null && valueTimestamp.size() == 2) {
                Integer valor = Integer.parseInt(valueTimestamp.get(0));
                long timestamp = Long.parseLong(valueTimestamp.get(1));
                return new Mensagem("GET", chave, valor, timestamp, meuIP, minhaPorta);
            }
            return null;
        }
    }

    public static void main(String[] args) {
        Servidor servidor = new Servidor();
        servidor.iniciar();
    }

    // Classe interna para atender conexões
    // Cada conexão é tratada em uma thread separada
    // Isso permite que o servidor atenda múltiplos clientes simultaneamente
    private class Atendente extends Thread {
        private Socket socket;

        public Atendente(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            tratarConexao(socket);
        }
    }
}