import java.util.*;

public class B2 {
    private static int N = 100;
    private static int times = 100_000;
    private static double Gama = 0.9;
    private static int NumThreads;
    private static double[][] f_i_j;

    public static int getNumThreads(){
        return NumThreads;
    }

    public static void main(String[] args){
        Scanner t = new Scanner(System.in);

        System.out.println("enter the number of cores you want (if maximum press 0)");
        int threads = t.nextInt();

        if (threads > 0){
            NumThreads = threads;
        }
        else{
            NumThreads = Runtime.getRuntime().availableProcessors();
        }

        if (NumThreads > N - 1) {
            System.out.println("Too many threads for N=" + N + ". Using " + (N - 1) + " instead.");
            NumThreads = N - 1;
        }

        System.out.println("solving B2 with "+NumThreads+" threads");

        f_i_j = new double[N+1][N+1];

        long start = System.currentTimeMillis();

        B2thread[] newthreads = new B2thread[NumThreads];
        for(int i = 0; i<NumThreads;i++){
            newthreads[i] = new B2thread(i);
            newthreads[i].start();
        }

        for (int i = 0; i < NumThreads; i++) {
            try {
                newthreads[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        long dur = System.currentTimeMillis() - start;
        System.out.println("Threads = " + NumThreads + ", time = " + dur + " ms");
    }
    static class B2thread extends Thread {
        private  int id;
        public B2thread(int id){
            this.id = id;
        }

        @Override
        public void run(){
            int numthreads = B2.getNumThreads();
            int interior = N-1;

            int base = interior/numthreads;
            int remainder = interior%numthreads;

            int currentRows;

            if (id < remainder) {
                currentRows = base + 1;
            } else {
                currentRows = base;
            }


            if(currentRows <=0){
                return;
            }

            int RowsBefore = id*base+Math.min(id,remainder);
            int RowStart = 1 + RowsBefore;
            int RowEnd = RowStart + currentRows-1;

            B2MsgPassing m = B2MsgPassing.getInstance();

            try{
                for(int i = 0; i<times; i++){
                    if(id > 0){
                        double[] first = new double[f_i_j[RowStart].length];
                        System.arraycopy(f_i_j[RowStart], 0, first, 0, f_i_j[RowStart].length);
                        m.send(id,id-1,first);
                    }
                    if (id < numthreads-1) {
                        double[] last = new double[f_i_j[RowEnd].length];
                        System.arraycopy(f_i_j[RowEnd], 0, last, 0, f_i_j[RowEnd].length);
                        m.send(id,id+1,last);
                    }

                    double[] RowAbove = null;
                    double[] RowBelow = null;

                    if(id > 0){
                        RowAbove = (double[]) m.receive(id, id-1);
                    }
                    if (id < numthreads-1) {
                        RowBelow = (double[]) m.receive(id, id+1);
                    }

                    for(int fi = RowStart; fi<= RowEnd; fi++){
                        for(int fj = 1; fj<N; fj++){
                            double g_i_j = g(fi,fj);
                            double current = f_i_j[fi][fj];

                            double north;
                            double south;
                            double east = f_i_j[fi][fj+1];
                            double west = f_i_j[fi][fj-1];

                            if(fi == RowStart){
                                if(RowAbove != null){
                                    north = RowAbove[fj];
                                }
                                else{
                                    north = f_i_j[fi-1][fj];
                                }
                            }
                            else{
                                north = f_i_j[fi-1][fj];
                            }
                            if(fi == RowEnd){
                                if(RowBelow != null){
                                    south = RowBelow[fj];
                                }
                                else{
                                    south = f_i_j[fi+1][fj];
                                }
                            }
                            else{
                                south = f_i_j[fi+1][fj];
                            }

                            f_i_j[fi][fj] = (1.0-Gama)*current + (Gama/4)*(north+south+west+east)
                                    - ((Gama*g_i_j)/4*(N*N));
                        }
                    }
                }
            }catch (RuntimeException e){
                e.printStackTrace();
            }
        }
        public double g(int i, int j){
            double x = (double) i / N;
            double y = (double) j / N;
            return -2.0 * (x * (1.0 - x) + y * (1.0 - y));
        }

    }

}
