// package startmain;
//
// import util.util;
//
// public class neoxiancheng implements Runnable {
//
// @Override
// public void run() {
// // TODO Auto-generated method stub
// while (true) {
// synchronized (util.neoqueue) {
// int poll = util.neoqueue.poll();
// System.out.println("出队的是" + poll);
// util.neoqueue.offer(poll);
// }
// try {
// Thread.sleep(200);
// } catch (InterruptedException e) {
// // TODO Auto-generated catch block
// e.printStackTrace();
// }
// }
// }
//
// }
