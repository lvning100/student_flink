package day02;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @Author: L.N
 * @Date: 2023/4/11 20:36
 * @Description: 有向无环图
 * 使用HashMap表示一张有向无环图
 */
public class Dag {
    public static void main(String[] args) {
        // HashMap
        // {
        // "A" : ["B","C"]
        // "B" : ["D","E"]
        // "C" : ["D","E"]
        // }
        //这种声明的方式在数据结构中叫做邻接表
        //声明一个HashMap
        HashMap<String, ArrayList<String>> dag = new HashMap<>();
        //定义A的邻接的顶点。并把A放入DAG中
        ArrayList<String> ANeighbors = new ArrayList<>();
        ANeighbors.add("B");
        ANeighbors.add("C");
        dag.put("A", ANeighbors);
        //定义B的邻接的顶点。并把B放入DAG中
        ArrayList<String> BNeighbors = new ArrayList<>();
        BNeighbors.add("D");
        BNeighbors.add("E");
        dag.put("B", BNeighbors);
        //定义C的邻接的顶点。并把C放入DAG中
        ArrayList<String> CNeighbors = new ArrayList<>();
        CNeighbors.add("D");
        CNeighbors.add("E");
        dag.put("C", CNeighbors);

        //遍历图的方式：拓扑排序
        //第一个参数是有向无环图，第二个参数是当前遍历的顶点，第三个参数是打印的结果
        topologicalSort(dag,"A","A");
    }

    //topologicalSort(dag,"A","A")
    //topologicalSort(dag,"A","A=>B")
    //topologicalSort(dag,"A","A=>B=>D")
    public static void topologicalSort(HashMap<String,ArrayList<String>> dag,String vertex,String result){
        //如果当前遍历的顶点是D或者E直接输出
        //如果不是D，E的顶点 继续遍历。并把每一次的结果合并字符串给result
        if (vertex.equals("D")||vertex.equals("E")){
            System.out.println(result);
        }else{
            //遍历vertex顶点指向所有顶点
            for (String v : dag.get(vertex)) {
                //递归调用
                topologicalSort(dag, v, result+" => "+ v);
            }
        }
    }
}
