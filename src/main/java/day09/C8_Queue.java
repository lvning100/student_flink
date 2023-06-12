package day09;

import java.util.ArrayList;

/**
 * @Author: L.N
 * @Date: 2023/5/4 22:44
 * @Description: 实现队列，入队和出队
 */
public class C8_Queue {
    public static void main(String[] args) {
        Queue queue = new Queue(2);
        queue.inQueue(1);
        queue.inQueue(2);
        queue.deQueue();
        queue.inQueue(3);
    }

    public static class Queue{
        //设置队列的大小
        public int size;
        //设置队列的的存放数组
        public ArrayList<Integer> queue;

        public Queue(int size){
            this.size = size;
            this.queue = new ArrayList<>();
        }

        public void inQueue(Integer i){
            if (queue.size() == size){
                throw new RuntimeException("队列已满");
            }
            queue.add(i);
        }

        public Integer deQueue(){
            if (queue.size() == 0){
                throw new RuntimeException("队列已空");
            }
            Integer result = queue.get(0);
            queue.remove(0);
            return result;
        }
    }
}
