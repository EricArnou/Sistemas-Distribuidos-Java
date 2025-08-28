public class Mensagem {
    private String requisicao; 
    private String chave;
    private Integer valor;
    private Long timestamp;
    private String ip;
    private int porta;

    public Mensagem() {} // construtor vazio necessário pro Gson

    // Mensagem PUT
    public Mensagem(String requisicao, String chave, Integer valor, String ip, int porta) {
        this.requisicao = requisicao;
        this.chave = chave;
        this.valor = valor;
        this.ip = ip;
        this.porta = porta;
    }

    // Mensagem Replication
    public Mensagem(String string, String chave, Integer valor, long timestamp) {
        this.requisicao = string;
        this.chave = chave;
        this.valor = valor;
        this.timestamp = timestamp;
    }

    // Mensagem PUT com timestamp e Retorno GET
    public Mensagem(String requisicao, String chave, Integer valor, Long timestamp, String meuIP, int minhaPorta) {
        this.requisicao = requisicao;
        this.chave = chave;
        this.valor = valor;
        this.timestamp = timestamp;
        this.ip = meuIP;
        this.porta = minhaPorta;
    }

    // Mensagem Wait_for_Response
    public Mensagem(String requisicao) {
        this.requisicao = requisicao;
    }

    // Mensagem GET
    public Mensagem(String requisicao, String chave2, Long timeMillis, String meuIP, int minhaPorta) {
        this.requisicao = requisicao;
        this.chave = chave2;
        this.timestamp = timeMillis;
        this.ip = meuIP;
        this.porta = minhaPorta;
    }

    // Getters e Setters
    public String getRequisicao() {
        return requisicao;
    }

    public void setRequisicao(String requisicao) {
        this.requisicao = requisicao;
    }

    public String getChave() {
        return chave;
    }

    public void setChave(String chave) {
        this.chave = chave;
    }

    public Integer getValor() {
        return valor;
    }

    public void setValor(Integer valor) {
        this.valor = valor;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPorta() {
        return porta;
    }

    public void setPorta(int porta) {
        this.porta = porta;
    }
}
