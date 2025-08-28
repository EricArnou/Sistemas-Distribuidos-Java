import java.io.*;
import java.net.*;
import java.util.*;
import com.google.gson.Gson;

public class Cliente {

    private List<String> ipServidores = new ArrayList<>();
    private List<Integer> portaServidores = new ArrayList<>();
    private Scanner scanner = new Scanner(System.in);
    private Random random = new Random();
    private String meuIP;
    private int minhaPorta;
    private Map<String, List<String>> bancoKvCliente = new HashMap<>();

    private final int QUANTIDADE_SERVIDORES = 3;
    private final Gson gson = new Gson();

    public static void main(String[] args) {
        Cliente cliente = new Cliente();
        cliente.iniciar();
    }

    public void iniciar() {

        System.out.println("=== Cliente ===");

        try {
            ServerSocket serverSocket = new ServerSocket(0); // 0 escolhe uma porta livre automaticamente
            minhaPorta = serverSocket.getLocalPort();
            meuIP = InetAddress.getLoopbackAddress().getHostAddress();

            // inicia um listener para receber mensagens dos servidores
            new Listener(serverSocket, this).start();

        } catch (IOException e) {
            System.out.println("Erro ao iniciar listener no cliente.");
            return;
        }

        int opcao;

        //menu interativo para o usuário
        do {

            System.out.print("\nEscolha: ");
            System.out.println("\n[1] INIT");
            System.out.println("[2] PUT");
            System.out.println("[3] GET");
            
            opcao = Integer.parseInt(scanner.nextLine().trim());

            switch (opcao) {
                case 1:
                    inicializar();
                    break;
                case 2:
                    realizarPut();
                    break;
                case 3:
                    realizarGet();
                    break;
                default:
                    System.out.println("Opção inválida.");
            }
        } while (true);
    }

    // Método para inicializar o cliente, solicitando IPs e portas dos servidores
    private void inicializar() {

        for (int i = 0; i < QUANTIDADE_SERVIDORES; i++) {
            System.out.print("Informe IP do servidor " + (i + 1) + ": ");
            String ip = scanner.nextLine().trim();
            System.out.print("Informe a porta do servidor " + (i + 1) + ": ");
            int porta = Integer.parseInt(scanner.nextLine().trim());
            ipServidores.add(ip);
            portaServidores.add(porta);
        }
    }

    // Método para realizar PUT no servidor
    private void realizarPut() {
        System.out.print("Chave: ");
        String chave = scanner.nextLine().trim();
        System.out.print("Valor: ");
        Integer valor = Integer.parseInt(scanner.nextLine());

        // Cria uma nova mensagem de PUT com os dados fornecidos
        Mensagem msg = new Mensagem("PUT", chave, valor, meuIP, minhaPorta);
        enviarMensagemPut(msg);
    }

    // Método para realizar GET no servidor
    private void realizarGet() {
        System.out.print("Chave: ");
        String chave = scanner.nextLine().trim();

        // Cria uma nova mensagem de GET com a chave fornecida
        Mensagem msg = new Mensagem("GET", chave, ultimoTimestampAssociado(chave), meuIP, minhaPorta);
        enviarMensagemGet(msg);
    }

    // Método para enviar mensagem GET para um servidor aleatório
    private void enviarMensagemGet(Mensagem msg) {
        int idx = escolherServidorAleatorio();
        String ip = ipServidores.get(idx);
        int porta = portaServidores.get(idx);

        // Tenta abrir uma conexão com o servidor e enviar a mensagem
        try (
                Socket socket = new Socket(ip, porta);
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));) {
            
            String json = gson.toJson(msg);
            
            // Envia a mensagem em formato JSON
            out.writeBytes(json + "\n");

        } catch (IOException e) {
            System.out.println("Erro de comunicação com servidor " + ip + ":" + porta);
        }
    }

    // Método para enviar mensagem PUT para um servidor aleatório
    private void enviarMensagemPut(Mensagem msg) {
        int idx = escolherServidorAleatorio();
        String ip = ipServidores.get(idx);
        int porta = portaServidores.get(idx);

        // Tenta abrir uma conexão com o servidor e enviar a mensagem
        try (
                Socket socket = new Socket(ip, porta);
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));) {

            String json = gson.toJson(msg);

            // Envia a mensagem em formato JSON
            out.writeBytes(json + "\n");

        } catch (IOException e) {
            System.out.println("Erro de comunicação com servidor " + ip + ":" + porta);
        }
    }

    // Método para armazenar chave e valor no banco de dados local do cliente
    private void putBancoCliente(Mensagem msg) {

        // utiliza synchronized para evitar problemas de concorrência
        synchronized (bancoKvCliente) {

            // Verifica se a chave já existe no banco de dados local do cliente
            // Se existir, atualiza o valor e o timestamp
            if (bancoKvCliente.containsKey(msg.getChave())) {
                bancoKvCliente.get(msg.getChave()).set(0, String.valueOf(msg.getValor()));
                bancoKvCliente.get(msg.getChave()).set(1, String.valueOf(msg.getTimestamp()));

            // Se não existir, cria uma nova entrada com a chave, valor e timestamp
            } else {
                List<String> valores = new ArrayList<>();
                valores.add(String.valueOf(msg.getValor()));
                valores.add(String.valueOf(msg.getTimestamp()));
                bancoKvCliente.put(msg.getChave(), valores);
            }
        }
    }

    // Método para obter o valor associado a uma chave do banco de dados local do cliente
    private List<String> getBancoCliente(String chave) {

        // utiliza synchronized para evitar problemas de concorrência
        synchronized (bancoKvCliente) {

            // Verifica se a chave existe no banco de dados local do cliente
            if (bancoKvCliente.containsKey(chave)) {
                return bancoKvCliente.get(chave);
            }
            
            // Se a chave não existir, retorna null
            return null;
        }
    }

    // Método para escolher um servidor aleatório
    private Integer escolherServidorAleatorio() {
        return random.nextInt(QUANTIDADE_SERVIDORES);
    }

    // Método para obter o último timestamp associado a uma chave
    private Long ultimoTimestampAssociado(String chave) {
        List<String> valores = getBancoCliente(chave);
        if (valores != null && !valores.isEmpty()) {
            return Long.parseLong(valores.get(1));
        }
        return 0L;
    }

    // Class Listener para receber mensagens dos servidores
    // Esta classe estende Thread para permitir que o cliente escute conexões de forma assíncrona
    private static class Listener extends Thread {

        private ServerSocket serverSocket;
        private Cliente cliente;
        private final Gson gson = new Gson();

        public Listener(ServerSocket serverSocket, Cliente cliente) {
            this.serverSocket = serverSocket;
            this.cliente = cliente;
        }

        @Override
        public void run() {

            while (true) {

                // Aguarda conexões dos servidores
                try (
                    Socket socket = serverSocket.accept();
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

                    String respostaJson = in.readLine();

                    // Verifica se a resposta não é nula    
                    if (respostaJson != null) {
                        Mensagem resposta = gson.fromJson(respostaJson, Mensagem.class);

                        switch (resposta.getRequisicao()) {
                            case "GET":

                                // Verifica se o valor da chave é nulo
                                // Se for nulo, significa que a chave não existe no servidor  
                                if (resposta.getValor() == null) {
                                    System.out.println("\nGET key: " + resposta.getChave() +
                                        " value: " + resposta.getValor() + " obtida do servidor " + resposta.getIp() + ":" + resposta.getPorta() +
                                        " meu timestamp: " + cliente.ultimoTimestampAssociado(resposta.getChave()) + " e do servidor: " + resposta.getTimestamp());
                                    continue;
                                } else {
                                    // Se o valor não for nulo, significa que a chave existe no servidor
                                    System.out.println("\nGET key: " + resposta.getChave() +
                                        " value: " + resposta.getValor() + " obtida do servidor " + resposta.getIp() + ":" + resposta.getPorta() +
                                        " meu timestamp: " + cliente.ultimoTimestampAssociado(resposta.getChave()) + " e do servidor: " + resposta.getTimestamp());
                                    
                                    // Armazena a chave e o valor no banco de dados local do cliente
                                    cliente.putBancoCliente(resposta);
                                }
                                break;
                            case "PUT_OK":

                                // Exibe a confirmação de PUT_OK com os detalhes da chave, valor e timestamp
                                    System.out.println("\nPUT_OK" + " key: " + resposta.getChave() +
                                    " value: " + resposta.getValor() + " timestamp: " + resposta.getTimestamp() +
                                    " realizada no servidor: " + resposta.getIp() + ":" + resposta.getPorta());

                                    // Armazena a chave e o valor no banco de dados local do cliente
                                    cliente.putBancoCliente(resposta);
                                    break;

                            case "WAIT FOR RESPONSE : GET":

                                // Exibe a resposta de WAIT FOR RESPONSE com os detalhes da chave, valor e timestamp
                                    System.out.println("\nRETORNO DO WAIT FOR RESPONSE: GET key: "
                                        + resposta.getChave() + " value: " + resposta.getValor() + " obtido do servidor: " + resposta.getIp() +
                                        ":" + resposta.getPorta() + " meu timestamp: " + cliente.ultimoTimestampAssociado(resposta.getChave()) +
                                        " e do servidor: " + resposta.getTimestamp());

                                    cliente.putBancoCliente(resposta);
                                break;

                            case "WAIT_FOR_RESPONSE":
                                // Exibe a confirmação de WAIT_FOR_RESPONSE
                                System.out.println("\nWAIT FOR RESPONSE");
                        
                            default:
                                break;
                        }
                    }

                } catch (IOException e) {
                    System.out.println("Erro ao receber resposta: " + e.getMessage());
                }
            }
        }
    }
}
