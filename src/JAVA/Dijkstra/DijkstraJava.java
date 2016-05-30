package JAVA.Dijkstra;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by yml on 16/5/17.
 */
public class DijkstraJava {
    private static int M = 10000; //此路不通
    public static void main(String[] args) throws IOException {
        int[][] weight = GetMatrix("/Users/yml/IdeaProjects/GraphExample/input/case1.csv");

        int start = 0;
        int[] shortPath = dijkstra(weight,start);

        for(int i = 0;i < shortPath.length;i++)
            System.out.println("从"+start+"出发到"+i+"的最短距离为："+shortPath[i]);


    }

    public static int[] dijkstra(int[][] weight, int start) {
        //接受一个有向图的权重矩阵，和一个起点编号start（从0编号，顶点存在数组中）
        //返回一个int[] 数组，表示从start到它的最短路径长度
        int n = weight.length;      //顶点个数
        int[] shortPath = new int[n];  //保存start到其他各点的最短路径
        String[] path = new String[n];  //保存start到其他各点最短路径的字符串表示
        for(int i=0;i<n;i++)
            path[i]=new String(start+"-->"+i);
        int[] visited = new int[n];   //标记当前该顶点的最短路径是否已经求出,1表示已求出

        //初始化，第一个顶点已经求出
        shortPath[start] = 0;
        visited[start] = 1;

        for(int count = 1; count < n; count++) {   //要加入n-1个顶点
            int k = -1;        //选出一个距离初始顶点start最近的未标记顶点
            int dmin = Integer.MAX_VALUE;
            for(int i = 0; i < n; i++) {
                if(visited[i] == 0 && weight[start][i] < dmin) {
                    dmin = weight[start][i];
                    k = i;
                }
            }

            //将新选出的顶点标记为已求出最短路径，且到start的最短路径就是dmin
            shortPath[k] = dmin;
            visited[k] = 1;

            //以k为中间点，修正从start到未访问各点的距离
            for(int i = 0; i < n; i++) {
                if(visited[i] == 0 && weight[start][k] + weight[k][i] < weight[start][i]) {
                    weight[start][i] = weight[start][k] + weight[k][i];
                    path[i] = path[k] + "-->" + i;
                }
            }
        }
        for(int i = 0; i < n; i++) {
            System.out.println("从"+start+"出发到"+i+"的最短路径为："+path[i]);
        }
        System.out.println("=====================================");
        return shortPath;

    }
    public static  int[][] GetMatrix(String fileName)throws IOException {

            List<Integer> NodeList = new ArrayList<>();//存储点的个数
            List<List<Integer>> VerList = new ArrayList<List<Integer>>(); //存储边的关系
            FileReader fr = new FileReader(fileName);
            BufferedReader bf = new BufferedReader(fr);
            String Line = "";
            while((Line = bf.readLine())!=null){
                String []records = Line.split(",");
                if(!NodeList.contains(Integer.valueOf(records[1]))){
                    NodeList.add(Integer.valueOf(records[1]));
                }
                if(!NodeList.contains(Integer.valueOf(records[2]))){
                    NodeList.add(Integer.valueOf(records[2]));
                }
                List<Integer> list = new ArrayList<Integer>();
                list.add(Integer.valueOf(records[1]));
                list.add(Integer.valueOf(records[2]));
                list.add(Integer.valueOf(records[3]));
                VerList.add(list);
            }
            int [][]weight = new int[NodeList.size()][NodeList.size()];
            for(int i=0;i<NodeList.size();i++){
                for(int j=0;j<NodeList.size();j++){
                    if(i!=j)
                        weight[i][j] = M;
                    else
                        weight[i][j] = 0;
                }
            }
            for(List<Integer> list:VerList){
                int start = list.get(0);
                int end = list.get(1);
                int time = list.get(2);
                weight[start][end] = time;
            }
            bf.close();
            fr.close();
            return weight;


    }

}
