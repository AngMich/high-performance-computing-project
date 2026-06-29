import java.util.*;
public class B2MsgPassing {

    public static B2MsgPassing instance = null;

    private static int max_value = 10000;

    private int max_size;
    private List<Row> row;

    private B2MsgPassing(int max_size){
        this.max_size = max_size;
        this.row = new ArrayList<>();
    }

    public static synchronized B2MsgPassing getInstance() {
        if(instance == null){
            instance = new B2MsgPassing(max_value);
        }
        return instance;
    }

    public synchronized void send(int current_id, int to_id, Object data){
        while(row.size()==max_size){
            try{
                wait();
            } catch (InterruptedException e){
                Thread.currentThread().interrupt();
            }
        }
        row.add(new Row(current_id, to_id, data));
        notifyAll();
    }

    public synchronized Object receive(int current_id, int from_id){
        while(true){
            for (int i=0;i < row.size();i++){
                Row r = row.get(i);
                if(r.sender == from_id && r.receiver == current_id){
                    Object data = r.data;
                    row.remove(i);
                    notifyAll();
                    return data;
                }
            }
            try{
                wait();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
