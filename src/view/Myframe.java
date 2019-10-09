package view;

import java.awt.Font;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;

import actionlisten.Actionlisten;






public class Myframe extends JFrame {

	public Actionlisten actionlisten;//监听事件
public JLabel lb_url;//鏄剧ず褰撳墠杩炴帴鐨剈rl
public JButton btn_submit,btn_test;
public JButton tantest;
public JTextArea  jf;
public JPanel panelOutput;  
public static Myframe myframe;
public static Myframe getMyFrame(){
	if(myframe==null){
		myframe=new Myframe();
	}
	return myframe;
}

private Myframe() {
	actionlisten=new Actionlisten();

	btn_submit=new JButton("开始运行");
	btn_submit.setFont(new Font("宋体", Font.PLAIN, 25));	
	
	btn_test=new JButton("测试参数");
	btn_test.setFont(new Font("宋体", Font.PLAIN, 25));	
	
	jf=new JTextArea(5,6);
	jf.setFont(new Font("宋体", Font.PLAIN, 25));
	jf.setLineWrap(true);        //激活自动换行功能 
	jf.setWrapStyleWord(true);            // 激活断行不断字功能  
	JScrollPane jsp = new JScrollPane(jf);    //添加滚动条   	
	
	
	
	this.setSize(900, 700);



	this.add(jsp);
	this.setLayout(null);
	//this.add(btn_submit);



	btn_submit.setBounds(350, 600, 200, 50);
	jsp.setBounds(30, 50, 800, 500);
	btn_submit.addActionListener(actionlisten);
	btn_submit.setActionCommand("start");
	
	//this.add(btn_test);
	btn_test.setBounds(450, 600, 200, 50);
	jsp.setBounds(30, 50, 800, 500);
	btn_test.addActionListener(actionlisten);
	btn_test.setActionCommand("test");
	

	
	this.setTitle("陕西预警数据比对抽取");
	this.setResizable(false);
	this.setLocationRelativeTo(null);
	 this.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
	this.setVisible(true);	
	
	
	
}
	

	
	
	
}
