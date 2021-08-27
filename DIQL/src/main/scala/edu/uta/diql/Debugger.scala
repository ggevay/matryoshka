/*
 * Copyright © 2017 University of Texas at Arlington
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uta.diql.core

import edu.uta.diql.{LiftedResult,ResultValue,ErasedValue,ErrorValue,Lineage,UnaryLineage,BinaryLineage}
import javax.swing._
import javax.swing.JTree._
import javax.swing.tree._
import javax.swing.event._
import java.awt.{List=>_,_}
import java.awt.event._


/* The DIQL debugger that uses the lineage generated in Provenance.scala */
class Debugger[T] ( val dataset: Array[LiftedResult[T]], val exprs: List[String] )
         extends JPanel(new GridLayout(1,0)) with TreeSelectionListener {
  val operators = List("flatMap","groupBy","orderBy","coGroup","cross","merge","repeat","reduce")
  val search = new JTextField(20)
  val frame = new JFrame("DIQL debugger")
  val toolBar = new JToolBar("")
  var exit = false
  var trace_nodes_only = false
  toolBar.setPreferredSize(new Dimension(1000,40))
  val button = new JButton()
  button.setActionCommand("Exit")
  button.setText("exit")
  val nframe: JFrame = frame
  button.addActionListener(new ActionListener() {
    def actionPerformed ( e: ActionEvent ) {
      nframe.setVisible(false)
      nframe.dispose()
      exit = true
    }
  })
  button.setVisible(true)
  toolBar.add(button)
  toolBar.add(Box.createRigidArea(new Dimension(200,0)))
  val checkb = new JCheckBox()
  checkb.setActionCommand("traceOnly")
  checkb.setText("trace nodes only")
  checkb.addActionListener(new ActionListener() {
    def actionPerformed ( e: ActionEvent ) {
      trace_nodes_only = !trace_nodes_only
      if (trace_nodes_only) {
        search.setText("")
        val model = tree.getModel.asInstanceOf[DefaultTreeModel]
        val root = createNode("","")
        root.setUserObject("results with trace and input nodes only")
        model.setRoot(root)
        model.reload(root)
      } else reset()
    }
  })
  checkb.setVisible(true)
  // toolBar.add(checkb)
  val button2 = new JButton()
  button2.setActionCommand("Previous")
  button2.setText("prev")
  button2.addActionListener(new ActionListener() {
    def actionPerformed ( e: ActionEvent ) {
    }
  })
  button2.setVisible(false)
  //toolBar.add(button2)
  val button3 = new JButton()
  button3.setActionCommand("Next")
  button3.setText("next")
  button3.addActionListener(new ActionListener() {
    def actionPerformed ( e: ActionEvent ) {
    }
  })
  button3.setVisible(false)
  //toolBar.add(button3)
  toolBar.add(Box.createRigidArea(new Dimension(200,0)))
  val button4 = new JButton()
  button4.setActionCommand("Clear")
  button4.setText("clear")
  button4.addActionListener(new ActionListener() {
    def actionPerformed ( e: ActionEvent ) {
      reset()
    }
  })
  toolBar.add(button4)
  toolBar.add(search)
  val button5 = new JButton()
  button5.setActionCommand("SearchOutput")
  button5.setText("Search Output")
  button5.addActionListener(new ActionListener() {
    def actionPerformed ( e: ActionEvent ) {
      if (!search.getText().equals("")) {
        val model = tree.getModel.asInstanceOf[DefaultTreeModel]
        val root = createNode(search.getText(),"")
        root.setUserObject("search output results")
        model.setRoot(root)
        model.reload(root)
      }
    }
  })
  toolBar.add(button5)
  val button6 = new JButton()
  button6.setActionCommand("SearchInput")
  button6.setText("Search Input")
  button6.addActionListener(new ActionListener() {
    def actionPerformed ( e: ActionEvent ) {
      if (!search.getText().equals("")) {
        val model = tree.getModel.asInstanceOf[DefaultTreeModel]
        val root = createNode("",search.getText())
        root.setUserObject("search input results")
        model.setRoot(root)
        model.reload(root)
      }
    }
  })
  toolBar.add(button6)
  val tree = new JTree(createNode("",""))
  import ScrollPaneConstants._
  val sp = new JScrollPane(tree,VERTICAL_SCROLLBAR_AS_NEEDED,HORIZONTAL_SCROLLBAR_NEVER)
  sp.setPreferredSize(new Dimension(1000,2000))
  sp.getVerticalScrollBar.setPreferredSize(new Dimension(20,0))
  tree.getSelectionModel.setSelectionMode(TreeSelectionModel.SINGLE_TREE_SELECTION)
  val currentFont: Font = tree.getFont
  tree.setFont(new Font(currentFont.getName,
                        currentFont.getStyle,
                        (currentFont.getSize*1.5).asInstanceOf[Int]))
  val renderer = new DefaultTreeCellRenderer() {
    override def getTreeCellRendererComponent ( tree: JTree, value: AnyRef, sel: Boolean, exp: Boolean,
                                       leaf: Boolean, row: Int, hasFocus: Boolean ): Component = {
      super.getTreeCellRendererComponent(tree,value,sel,exp,leaf,row,hasFocus)
      val o = value.asInstanceOf[DefaultMutableTreeNode].getUserObject
      // error strings are red
      if (o.isInstanceOf[ErrorString])
         setForeground(Color.red)
      // erased strings are orange
      if (o.isInstanceOf[ErasedString])
         setForeground(Color.orange)
      // tagged strings are green
      if (o.isInstanceOf[TaggedString])
         setForeground(Color.green)
      this
    }
  }
  renderer.setLeafIcon(null)
  renderer.setOpenIcon(null)
  renderer.setClosedIcon(null)
  tree.setCellRenderer(renderer)
  tree.addTreeSelectionListener(this)
  setLayout(new BorderLayout())
  add(toolBar,BorderLayout.NORTH)
  add(sp,BorderLayout.CENTER)

  /** wrapped strings to be colored red */
  class ErrorString ( val value: String ) {
    override def toString: String = value
  }

  /** wrapped strings to be colored orange */
  class ErasedString ( val value: String ) {
    override def toString: String = value
  }

  /** wrapped strings to be colored green */
  class TaggedString ( val value: String ) {
    override def toString: String = value
  }

  def valueChanged ( e: TreeSelectionEvent ) { }

  def reset () {
    search.setText("")
    val model = tree.getModel.asInstanceOf[DefaultTreeModel]
    val label = model.getRoot.asInstanceOf[DefaultMutableTreeNode].getUserObject
    val root = createNode("","")
    root.setUserObject(label)
    model.setRoot(root)
    model.reload(root)
  }

  def existing_child ( node: DefaultMutableTreeNode, parent: DefaultMutableTreeNode ): Boolean = {
    val no = node.getUserObject
    val ns = if (no.isInstanceOf[TaggedString])
                no.asInstanceOf[TaggedString].value
             else no.asInstanceOf[String]
    for ( i <- 0 until parent.getChildCount ) {
      val co = parent.getChildAt(i).asInstanceOf[DefaultMutableTreeNode].getUserObject
      val cs = if (co.isInstanceOf[TaggedString])
                  co.asInstanceOf[TaggedString].value
               else co.asInstanceOf[String]
      if (cs.equals(ns))
         return true
    }
    false
  }

  def create_nodes ( lineage: Lineage, parent: DefaultMutableTreeNode, inputSearch: String, first: Boolean ) {
    val v = lineage.value
    val opr = exprs(lineage.tree)
    var node =  new DefaultMutableTreeNode(if (first) v.toString else opr+": "+v)
    if (first || !operators.contains(opr) || opr == "trace" || !trace_nodes_only)
       parent.add(node)
    else node = parent
    lineage match {
      case UnaryLineage(_,_,s)
        => s.foreach(create_nodes(_,node,inputSearch,false))
      case BinaryLineage(_,_,ls,rs)
        => ls.foreach(create_nodes(_,node,inputSearch,false))
           rs.foreach(create_nodes(_,node,inputSearch,false))
    }
    var matched = false
    for ( i <- 0 until node.getChildCount ) {
      val child = node.getChildAt(i).asInstanceOf[DefaultMutableTreeNode]
      matched = matched || child.getUserObject.isInstanceOf[TaggedString]
    }
    if (matched && node.getUserObject.isInstanceOf[String])
       node.setUserObject(new TaggedString(node.getUserObject.asInstanceOf[String]))
       else if (!operators.contains(opr))
               if (inputSearch != "" && v.toString.contains(inputSearch)
                   && node.getUserObject.isInstanceOf[String])
                  node.setUserObject(new TaggedString(node.getUserObject.asInstanceOf[String]))
  }

  def createNode ( outputSearch: String, inputSearch: String ): DefaultMutableTreeNode = {
    val node = new DefaultMutableTreeNode(exprs(dataset(0).lineage.tree))
    for ( e <- dataset )
        e match {
          case ResultValue(v,q)
            => if (outputSearch.equals("") || v.toString.contains(outputSearch))
                  create_nodes(q,node,inputSearch,true)
          case ErrorValue(msg,q)
            => val en =  new DefaultMutableTreeNode(msg)
               en.setUserObject(new ErrorString(en.getUserObject.asInstanceOf[String]))
               node.add(en)
               create_nodes(q,en,inputSearch,false)
          case ErasedValue(UnaryLineage(_,v,List(q)))
            => val en =  new DefaultMutableTreeNode("erased from flatMap: "+v)
               en.setUserObject(new ErasedString(en.getUserObject.asInstanceOf[String]))
               node.add(en)
               create_nodes(q,en,inputSearch,false)
        }
    node
  }

  def createAndShowGUI () {
    import WindowConstants._
    frame.setDefaultCloseOperation(DO_NOTHING_ON_CLOSE)
    frame.addWindowListener(new WindowListener() {
      def windowClosing ( e: WindowEvent ) {
        frame.setVisible(false)
        frame.dispose()
        exit = true
      }
      def windowActivated ( e: WindowEvent ) {}
      def windowClosed ( e: WindowEvent ) {}
      def windowDeactivated ( e: WindowEvent ) {}
      def windowDeiconified ( e: WindowEvent ) {}
      def windowIconified ( e: WindowEvent ) {}
      def windowOpened ( e: WindowEvent ) {}
    })
    frame.add(this)
    frame.pack()
    frame.setVisible(true)
  }

  /** use the DIQL debugger on a query result */
  def debug () {
    try {
      exit = false
      trace_nodes_only = false
      SwingUtilities.invokeLater(new Runnable() {
        def run () {
          createAndShowGUI()
        }
      })
      while (!exit)
        Thread.sleep(1000)
    } catch { case _: Exception => }
  }
}
