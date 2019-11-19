package Synopsis.Wavelets;

import java.io.*;
import java.util.Scanner;

import static Synopsis.Wavelets.Utils.relationship.*;


public class SiblingTree implements Serializable {
    transient HeapNode topheap;
    transient HeapNode parentheap, prevheap;
    transient HeapPlace freeplace;
    int nodecounter;
    transient PointerNode frontlinefirst;
    transient PointerNode highesthangingfrontline;
    transient DataNode highest;
    DataNode rootnode;

    transient DataNode singlenode, sibling;
    double globalaverage = 0;

    int space = 1048576;
    int size = 2097152;
    private final int INFINITY = 1000000000;
    transient Scanner in ;
    transient BufferedWriter bw;
    transient PrintWriter pw;
    int cnt;
    transient FileWriter fos;
    int globalNumber;

    public SiblingTree(int size, int space) throws Exception{
        this.size = size;
        this.space = space;
        topheap = null;
        parentheap = null;
        prevheap = null;
        freeplace = null;
        nodecounter = 0;
        globalNumber = 0;
        frontlinefirst = null;
        highesthangingfrontline = null;
        highest = null;
        rootnode = null;
        singlenode = null;
        sibling = null;
//        in = new Scanner(new File("D:\\Dropbox\\bdma\\tub\\thesis\\flink-distributed-wavelet\\src\\main\\resources\\input.txt"));
//        fos = new FileWriter(new File("D:\\Dropbox\\bdma\\tub\\thesis\\flink-distributed-wavelet\\src\\main\\resources\\output.txt"));
//        bw = new BufferedWriter(fos);
        topheap = new HeapNode(); // these are all global variables
        topheap.parent = null;
        topheap.left = null;
        topheap.right = null;
        topheap.next = null;
        topheap.previous = null;
        topheap.data = null;

        prevheap = topheap;
        parentheap = topheap;

        frontlinefirst = new PointerNode();
//frontlinefirst.previous = null;
        frontlinefirst.next = null;
        frontlinefirst.hangChild= null; // 14/2/2005
        frontlinefirst.errorhanging = false; // initialization 16/2/2005
        frontlinefirst.orderinlevel = 0;
        frontlinefirst.level = 1; // was missing, added 20/2/2005

//        pw = new PrintWriter(bw);
    }

    void setFos(String outputFile) throws Exception{
        fos = new FileWriter(new File(outputFile));
        bw = new BufferedWriter(fos);
    }

    void closeIO() throws Exception{
        bw.close();
//        pw.close();
//        fos.close();
    }


    int index(int levels, DataNode datatemp) {
        return (int)Math.pow(2,(levels - datatemp.level)) + datatemp.orderinlevel - 1;
    }

    int descendants(DataNode datatemp) {
        return (int)Math.pow(2,datatemp.level);
    }

    int past(DataNode datatemp) {
        return (datatemp.orderinlevel - 1)*descendants(datatemp);
    }

    int present(DataNode datatemp) {
        return (datatemp.orderinlevel)*descendants(datatemp);
    }


    void fixheapup(HeapNode heaptemp) {
        DataNode hold;

        if (heaptemp.parent == null) System.out.println("null parent");
        if (heaptemp.parent.data == null) System.out.println("null parent data" );
        if (heaptemp.data == null) System.out.println("null data");

//cout << "Bringing data " << heaptemp.data.index << " to heap position of " << heaptemp.parent.data.index << endl;


        while ((heaptemp.parent!=null) && (heaptemp.parent.data.maxabserror > heaptemp.data.maxabserror)) {
            //cout << "Bringing data " << heaptemp.data.index << " to heap position of " << heaptemp.parent.data.index << endl;

            hold = heaptemp.data;
            heaptemp.data = heaptemp.parent.data;
            heaptemp.data.heap = heaptemp;
            heaptemp.parent.data = hold;
            hold.heap = heaptemp.parent;
            heaptemp = heaptemp.parent;
        }
    }

    void deleteHeap(HeapNode heaptemp){
        heaptemp.data = prevheap.data;
        prevheap = prevheap.previous;
        updateheap(heaptemp);
//        if(heaptemp.parent != null && heaptemp.parent.data != null &&
//                heaptemp.data.maxabserror > heaptemp.parent.data.maxabserror){
//            // move up
//
//        }
//        else if(heaptemp.left != null && heaptemp.left.data != null && heaptemp.data.maxabserror > heaptemp.left.data.maxabserror){
//            // push down left
//        }
//        else if(heaptemp.right != null && heaptemp.right.data != null && heaptemp.data.maxabserror > heaptemp.right.data.maxabserror){
//            // push down right
//        }
    }

    void fixheapdown(HeapNode heaptemp /*, ofstream& outfile*/) { // re-activated 16/2/2005, discarding top, contraction
        int direction;
        HeapPlace prevfreeplace;

        while (((heaptemp.left != null) && (heaptemp.left.data != null)) ||
                ((heaptemp.right!=null) && (heaptemp.right.data != null))) {
            if (    (heaptemp.left!=null) &&
                    (heaptemp.left.data != null) &&
                    (
                            (heaptemp.right==null) ||
                                    (heaptemp.right.data == null) ||
                                    (
                                            (heaptemp.right!=null) &&
                                                    (heaptemp.left.data.maxabserror < heaptemp.right.data.maxabserror)
                                    )
                    )
            ) { // conditions refined 16/2/2005
                heaptemp.data = heaptemp.left.data;
                heaptemp.data.heap = heaptemp;
                heaptemp = heaptemp.left;
                direction = 1;
            } else {
                heaptemp.data = heaptemp.right.data;
                heaptemp.data.heap = heaptemp;
                heaptemp = heaptemp.right;
                direction = 2;
            }
        }

        heaptemp.data = null;

        if (freeplace == null) { // add very first item in the stack of free places
            freeplace = new HeapPlace();
            freeplace.freeHeapNode = heaptemp;
            freeplace.nextPlace = null;
            freeplace.previousPlace = null;
        } else if (freeplace.nextPlace != null) { // put free heapnode in the next position in stack
            freeplace = freeplace.nextPlace;
            freeplace.freeHeapNode = heaptemp;
        } else if (freeplace.nextPlace == null) { // create new position in stack and put heapnode there
            prevfreeplace = freeplace;
            freeplace = new HeapPlace();
            freeplace.freeHeapNode = heaptemp;
            freeplace.nextPlace = null;
            freeplace.previousPlace = prevfreeplace;
            prevfreeplace.nextPlace = freeplace;
        }
    }

    void updateheap(HeapNode heaptemp) { // re-activated 16/2/2005, update after maxabserror change in a node
        DataNode hold;

        if ((heaptemp.parent!=null) && (heaptemp.parent.data.maxabserror>heaptemp.data.maxabserror)) fixheapup(heaptemp);

        while (((heaptemp.left!=null) && (heaptemp.left.data != null) && (heaptemp.left.data.maxabserror<heaptemp.data.maxabserror))
                || ((heaptemp.right!=null) && (heaptemp.right.data != null) && (heaptemp.right.data.maxabserror<heaptemp.data.maxabserror))) {
            if (((heaptemp.left!=null) && (heaptemp.left.data != null))
                    && ((heaptemp.right==null) || (heaptemp.right.data == null) || ((heaptemp.right!=null)
                    && (heaptemp.left.data.maxabserror<heaptemp.right.data.maxabserror)))) { // conditions refined 16/2/2005
                hold = heaptemp.data;
                heaptemp.data = heaptemp.left.data;
                heaptemp.data.heap = heaptemp;
                heaptemp.left.data = hold;
                hold.heap = heaptemp.left;
                heaptemp = heaptemp.left;
            } else {
                hold = heaptemp.data;
                heaptemp.data = heaptemp.right.data;
                heaptemp.data.heap = heaptemp;
                heaptemp.right.data = hold;
                hold.heap = heaptemp.right;
                heaptemp = heaptemp.right;
            }
        }
    }


    double singleerror(DataNode datanode) {
        double error;
        double data = datanode.data; //,fabsv;
        double mostpositiveerrorleft, mostpositiveerrorright, mostnegativeerrorleft, mostnegativeerrorright;

        mostpositiveerrorleft=datanode.mostpositiveerrorleft;
        mostnegativeerrorleft=datanode.mostnegativeerrorleft;
        mostpositiveerrorright=datanode.mostpositiveerrorright;
        mostnegativeerrorright=datanode.mostnegativeerrorright;

//if (datanode.index==0) error = max(fabs(mostpositiveerrorleft - data),fabs(mostnegativeerrorleft - data)); else
        error = Math.max(Math.max(Math.abs(mostpositiveerrorleft - data),Math.abs(mostnegativeerrorleft - data)),
                Math.max(Math.abs(mostpositiveerrorright + data),Math.abs(mostnegativeerrorright + data)));

        return error;
    }

    Utils.relationship ancestor(DataNode datanode1, DataNode datanode2) {
        int leveldifference = datanode1.level - datanode2.level;

        int ancestor = datanode1.orderinlevel, descendant = datanode2.orderinlevel;

        int power = (int)Math.pow(2,leveldifference);

        int maxdescendant = ancestor*power;

        int mindescendant = maxdescendant - power + 1;

        float middledescendant = (float)(mindescendant + maxdescendant)/2;

        if (descendant > maxdescendant) return none;
        if (descendant < mindescendant) return none;

        if (descendant < middledescendant) return leftRel;
        else /*if (descendant > middledescendant)*/ return rightRel;
    }

    void propagateerrordown(DataNode patriarch, DataNode node, double data, Utils.relationship reltonode/*, ofstream& outfile*/)
// commented out, rewritten at 14/2/2005, divided in two, this is the off-shoot
    {
        double maxabserrorold;

//outfile << "Propagating error down ";
//if (reltonode == left) outfile << "left"; else if (reltonode == right) outfile << "right";
//outfile << " to " << node.index << endl;

        if (reltonode == leftRel) { // propagate error to left descendant
            node.mostpositiveerrorleft-=data;
            node.mostnegativeerrorleft-=data;
            node.mostpositiveerrorright-=data;
            node.mostnegativeerrorright-=data;
        } else if (reltonode == rightRel) { // propagate error to right descendant
            node.mostpositiveerrorleft+=data;
            node.mostnegativeerrorleft+=data;
            node.mostpositiveerrorright+=data;
            node.mostnegativeerrorright+=data;
        }

        maxabserrorold = node.maxabserror;
        node.maxabserror = singleerror(node); // added 16/2/2005

        if (node.maxabserror != maxabserrorold) { // added 16/2/2005
            updateheap(node.heap);
            //outfile << "Re-adjusted heap, topheap now points to " << topheap.data.index << " of error " << topheap.data.maxabserror << endl;
        }

        if (node.leftChild != null) { //&& (node.index<size/2)
            propagateerrordown(patriarch,node.leftChild,data,reltonode/*,outfile*/);
        }

        if (node.nextSibling != null) {
            propagateerrordown(patriarch,node.nextSibling,data,reltonode/*,outfile*/);
        }
    }

    void propagateerrordownfirstlevel(DataNode patriarch, DataNode node, double data, Utils.relationship reltonode/*, ofstream& outfile*/)
// commented out, rewritten at 14/2/2005
    {
        double maxabserrorold;

//outfile << "Propagating error down ";
//if (reltonode == left) outfile << "left"; else if (reltonode == right) outfile << "right";
//outfile << " to " << node.index << endl;

//if ((patriarch.index == 12) && (node.index == 25))
//{
        //outfile << "Propagating error down from 12 of error data " << data << " now to " << node.index << " of current maxerror " << node.maxabserror << " and relationship " << reltonode << endl;
//}

        if (reltonode == leftRel) { // propagate error to left descendant
            node.mostpositiveerrorleft-=data;
            node.mostnegativeerrorleft-=data;
            node.mostpositiveerrorright-=data;
            node.mostnegativeerrorright-=data;
        } else if (reltonode == rightRel) { // propagate error to right descendant
            node.mostpositiveerrorleft+=data;
            node.mostnegativeerrorleft+=data;
            node.mostpositiveerrorright+=data;
            node.mostnegativeerrorright+=data;
        }


        maxabserrorold = node.maxabserror;
        node.maxabserror = singleerror(node); // added 16/2/2005

        if (node.maxabserror != maxabserrorold) { // added 16/2/2005
            updateheap(node.heap);
            //outfile << "Re-adjusted heap, topheap now points to " << topheap.data.index << " of error " << topheap.data.maxabserror << endl;
        }


        if (node.leftChild != null) { //&& (node.index<size/2)
            propagateerrordown(patriarch,node.leftChild,data,reltonode/*,outfile*/);
        }

        if (node.nextSibling!= null) {
            propagateerrordownfirstlevel(patriarch,node.nextSibling,data,node.nextSibling.reltoparent /*ancestor(patriarch,node.nextsibling)*//*,outfile*/);
        }
    }

    void propagateerrorup(DataNode node, DataNode child,
                          double positiveerror, double negativeerror, double oldpositiveerror, double oldnegativeerror/*,
					  ofstream& outfile*/) {
        int change=0;
        double maxabserrorold;
        double data = node.data;
        double oldvaluepos, oldvalueneg;
        DataNode sibling;
        double poserror, negerror;
        double oldpositiveerrorhere, oldnegativeerrorhere;

        oldpositiveerrorhere = Math.max(node.mostpositiveerrorleft, node.mostpositiveerrorright);
        oldnegativeerrorhere = Math.min(node.mostnegativeerrorleft, node.mostnegativeerrorright);

//outfile << "Propagating error up to " << node.index << endl;

        if (child.reltoparent==leftRel) { // new function developed 25/2/2005
            if ((oldpositiveerror == node.mostpositiveerrorleft) && (oldpositiveerror > positiveerror)) {
                //cout << "It happened!" << endl;

                oldvaluepos = node.mostpositiveerrorleft;
                sibling = node.leftChild;
                poserror = -INFINITY;
                while (sibling != null) {
                    if ((sibling.reltoparent == leftRel) && (sibling != child)) {
                        poserror = Math.max(poserror,Math.max(sibling.mostpositiveerrorleft, sibling.mostpositiveerrorright));
                    }
                    sibling = sibling.nextSibling;
                }
                node.mostpositiveerrorleft = Math.max(poserror,positiveerror);
                if (oldvaluepos != node.mostpositiveerrorleft) change=1;
            } else {
                if (positiveerror > node.mostpositiveerrorleft) {
                    node.mostpositiveerrorleft = positiveerror;
                    change = 1;
                }
            }

            if ((oldnegativeerror == node.mostnegativeerrorleft) && (oldnegativeerror < negativeerror)) {
                //cout << "It happened!" << endl;

                oldvalueneg = node.mostnegativeerrorleft;
                sibling = node.leftChild;
                negerror = INFINITY;
                while (sibling != null) {
                    if ((sibling.reltoparent == leftRel) && (sibling != child)) {
                        negerror = Math.min(negerror,Math.min(sibling.mostnegativeerrorleft, sibling.mostnegativeerrorright));
                    }
                    sibling = sibling.nextSibling;
                }
                node.mostnegativeerrorleft = Math.min(negerror,negativeerror);
                if (oldvalueneg != node.mostnegativeerrorleft) change=1;
            } else {
                if (negativeerror < node.mostnegativeerrorleft) {
                    node.mostnegativeerrorleft = negativeerror;
                    change = 1;
                }
            }
        } else {
            if ((oldpositiveerror == node.mostpositiveerrorright) && (oldpositiveerror > positiveerror)) {
                //cout << "It happened!" << endl;

                oldvaluepos = node.mostpositiveerrorright;
                sibling = node.leftChild;
                poserror = -INFINITY;
                while (sibling != null) {
                    if ((sibling.reltoparent == rightRel) && (sibling != child)) {
                        poserror = Math.max(poserror,Math.max(sibling.mostpositiveerrorleft, sibling.mostpositiveerrorright));
                    }
                    sibling = sibling.nextSibling;
                }
                node.mostpositiveerrorright = Math.max(poserror,positiveerror);
                if (oldvaluepos != node.mostpositiveerrorright) change=1;
            } else {
                if (positiveerror > node.mostpositiveerrorright) {
                    node.mostpositiveerrorright = positiveerror;
                    change = 1;
                }
            }

            if ((oldnegativeerror == node.mostnegativeerrorright) && (oldnegativeerror < negativeerror)) {
                //cout << "It happened!" << endl;

                oldvalueneg = node.mostnegativeerrorright;
                sibling = node.leftChild;
                negerror = INFINITY;
                while (sibling != null) {
                    if ((sibling.reltoparent == rightRel) && (sibling != child)) {
                        negerror = Math.min(negerror,Math.min(sibling.mostnegativeerrorleft, sibling.mostnegativeerrorright));
                    }
                    sibling = sibling.nextSibling;
                }
                node.mostnegativeerrorright = Math.min(negerror,negativeerror);
                if (oldvalueneg != node.mostnegativeerrorright) change=1;
            } else {
                if (negativeerror < node.mostnegativeerrorright) {
                    node.mostnegativeerrorright = negativeerror;
                    change = 1;
                }
            }
        }

        if (change == 1) {
            maxabserrorold = node.maxabserror;
            node.maxabserror = singleerror(node); // added 16/2/2005 here is the right position for this

            if (node.maxabserror != maxabserrorold) { // added 16/2/2005
                updateheap(node.heap);
                //outfile << "Re-adjusted heap, topheap now points to " << topheap.data.index << " of error " << topheap.data.maxabserror << endl;
            }

            //outfile << "New error values on " << node.index << " are left " << node.mostpositiveerrorleft << " and "
            // << node.mostnegativeerrorleft << " right " << node.mostpositiveerrorright << " and " << node.mostnegativeerrorright << endl;
        }

        if (node.parent != null) { //&& (node.index>0)) // 15/2/2005 no null parent
            positiveerror = Math.max(node.mostpositiveerrorleft, node.mostpositiveerrorright);
            negativeerror = Math.min(node.mostnegativeerrorleft, node.mostnegativeerrorright);

            propagateerrorup(node.parent,node,positiveerror,negativeerror,oldpositiveerrorhere,oldnegativeerrorhere/*,outfile*/);
        }
//}
    }


    void propagateerror(DataNode node/*, ofstream& outfile*/) { // alternative error propagation function written 10/2/2005
        double data; //,fabsv;
        double mostpositiveerror, mostnegativeerror;
        double oldpositiveerror, oldnegativeerror;
        double mostpositiveerrorleft, mostnegativeerrorleft, mostpositiveerrorright, mostnegativeerrorright;
        DataNode sibling;

        data=node.data;

        mostpositiveerrorleft = node.mostpositiveerrorleft;
        mostnegativeerrorleft = node.mostnegativeerrorleft;
        mostpositiveerrorright = node.mostpositiveerrorright;
        mostnegativeerrorright = node.mostnegativeerrorright;

        oldpositiveerror = Math.max(mostpositiveerrorleft, mostpositiveerrorright);
        oldnegativeerror = Math.min(mostnegativeerrorleft, mostnegativeerrorright);

        mostpositiveerror = Math.max(mostpositiveerrorleft-data, mostpositiveerrorright+data);
        mostnegativeerror = Math.min(mostnegativeerrorleft-data, mostnegativeerrorright+data);

        fixheapdown(topheap/*, outfile*/); // re-activated 16/2/2005, threshold the top of the heap which is discarded from error-tree also

//outfile << "Discarded top, topheap now points to " << topheap.data.index << " of error " << topheap.data.maxabserror << endl;

        if (/*(node.leftchild == null) &&*/ (node.parent == null)) { // error cannot be propagated anywhere
            // this expression was wrong, corrected 25/2/2005
            if (node.hanged == null) {
                //outfile << "Node " << node.index << " is not hanging anywhere!" << endl;
                sibling = node;
                while (sibling.previousSibling!= null) sibling = sibling.previousSibling; // go to leftmostsibling
                if (sibling.hanged == null) {
                    //outfile << "Leftmost sibling of node " << node.index << " is not hanging anywhere either!" << endl;
                } else {
                    //outfile << "But its leftmost sibling is hanging somewhere!" << endl;
                    if (sibling.hanged.errorhanging == true) {
                        //outfile << "There is already hanging error at level " << sibling.hanged.level << endl;

                        sibling.hanged.positiveerror = Math.max(sibling.hanged.positiveerror, mostpositiveerror);
                        sibling.hanged.negativeerror = Math.min(sibling.hanged.negativeerror, mostnegativeerror);
                        //outfile << "Encountered Hanging error's relation to parent is " << sibling.hanged.reltoparenterrors << endl;
                    } else {
                        sibling.hanged.negativeerror = mostnegativeerror;
                        sibling.hanged.positiveerror = mostpositiveerror;

                        sibling.hanged.errorhanging = true;

                        sibling.hanged.reltoparenterrors = node.reltoparent;
                        //outfile << "Attributed Hanging error's relation to parent is " << sibling.hanged.reltoparenterrors << endl;
                        //outfile << "Now there is hanging error at level " << sibling.hanged.level << endl;
                    }
                }
            } else {
                //outfile << "Hopefully node " << node.index << " is hanging well" << endl;

                node.hanged.negativeerror = mostnegativeerror; // not with pointer 16/2/2005
                node.hanged.positiveerror = mostpositiveerror;

                node.hanged.errorhanging = true;

                node.hanged.reltoparenterrors = node.reltoparent;
                //outfile << "Attributed Hanging node's relation to parent is " << node.hanged.reltoparenterrors << endl;
                //outfile << "Now there is hanging error at level " << node.hanged.level << endl;
            }
        }

        if (node.leftChild != null) //&& (node.index < size/2))
            propagateerrordownfirstlevel(node,node.leftChild, data, node.leftChild.reltoparent /*ancestor(node,node.leftchild)*/ /*,outfile*/); // 14/2/2005

        if (node.parent != null) { //(node.index>0) // cannot send to null parent! Dynamic version 15/2/2005
            propagateerrorup(node.parent, node, mostpositiveerror, mostnegativeerror, oldpositiveerror, oldnegativeerror/*, outfile*/);
        }
    }

    // comprehensive climbing, written at 12/2/2005
    void climbup(int number, int index, double data1, double data2 /*, ofstream& outfile*/) {
        PointerNode frontlinenode, prevfrontlinenode;
        DataNode sibling, leftchild;
        DataNode climbnode, prevclimbnode;
        double climbvalue = 0, prevstoredvalue = 0, data;
        int justentered;
        double mostnegativeerrorleft, mostpositiveerrorleft, mostnegativeerrorright, mostpositiveerrorright;
        int level;

        HeapNode heaptemp; // added 16/2/2005

        frontlinenode = frontlinefirst;
        prevfrontlinenode = null;

        climbnode = null;
        justentered = 1;
        level = 0;
        while ((number>0) && (number % 2 == 0)) {
            //cout << "Going climbing one loop!" << endl;

            number /= 2;
            level++;

            index = (index-1) / 2; // get the index of next node
            //outfile << "Climbing on index " << index << endl;
            //cout << "Climbing on index " << index << endl;


            if (justentered==1) {
                climbvalue = (data1 + data2)/2;
                data = data1 - climbvalue;
                justentered = 0;
                leftchild = null; // for absolute error only!
            } else { //if (index != 0) //(climbnode.index != 0) // exception at the top 14/2/2005
                climbvalue = (climbvalue + prevstoredvalue)/2;
                data = prevstoredvalue - climbvalue;
                leftchild = prevfrontlinenode.hangChild;

                if (prevfrontlinenode.hangChild !=null) {
                    prevfrontlinenode.hangChild.hanged = null;// de-hang the hanged node also! 15/2/2005
                    prevfrontlinenode.hangChild = null; // 15/2/2005 nullify hangchild after using it above
                }
            }

            if (frontlinenode != null) {
                sibling = frontlinenode.hangChild;
            } else sibling = null;

            if (sibling != null) {
                //outfile << "Hangchild is " << sibling.index << endl;
                while (sibling.nextSibling != null) sibling = sibling.nextSibling;
            }
            //else outfile << "Hangchild is null" << endl;

            nodecounter++;

            prevclimbnode = climbnode;
            climbnode = new DataNode();
            climbnode.index = index;
            climbnode.level = level;
            climbnode.orderinlevel = number;

            //if (index == 0) rootnode = climbnode;

            //if (index == 13)
            //{
            // made13 = 1;
            // node13 = climbnode;
            //}

            climbnode.reltoparent = (number % 2 == 0) ? rightRel : leftRel;

            climbnode.hanged = null;

            //climbnode.value = climbvalue;

            climbnode.data = data;

            //outfile << "Climbing on node of error " << data << endl;

            //if (index != 0) climbnode.data = data; // 14/2/2005, better 15/2/2005
            //else climbnode.data = climbvalue;

            climbnode.leftChild = leftchild;
            climbnode.previousSibling = sibling;

            //outfile << "Hangchild for " << climbnode.index << " is ";
            //if (leftchild == null) outfile << "null" << endl; else outfile << leftchild.index << endl;

            //outfile << "Previoussibling for " << climbnode.index << " is ";
            //if (sibling == null) outfile << "null" << endl; else outfile << sibling.index << endl;

            if (sibling != null) sibling.nextSibling = climbnode; // connect both ways!

            //cout << "Treated leftchild, previoussibling!" << endl;

            climbnode.parent = null;
            climbnode.nextSibling = null;

            sibling = prevclimbnode; // connect all the children of created node to it
            while(sibling != null) {
                sibling.parent = climbnode;
                sibling.reltoparent = ancestor(climbnode,sibling);

                //if (sibling == prevclimbnode)
                //outfile << "Ancestor called for prevclimbnode and returned " << sibling.reltoparent << endl;
                //else
                //outfile << "Ancestor called for other sibling and returned " << sibling.reltoparent << endl;

                sibling = sibling.previousSibling;
            }


            if (prevclimbnode == null) {
                climbnode.mostnegativeerrorleft = 0;
                climbnode.mostpositiveerrorleft = 0;
                climbnode.mostnegativeerrorright = 0;
                climbnode.mostpositiveerrorright = 0;
            } else {
                mostnegativeerrorleft = INFINITY;
                mostpositiveerrorleft = -INFINITY;
                mostnegativeerrorright = INFINITY;
                mostpositiveerrorright = -INFINITY;

                //cout << "Going for error calculations!" << endl;

                sibling = prevclimbnode;
                while (sibling != null) {
                    if (sibling.reltoparent == leftRel) { //(ancestor(climbnode,sibling) == left)
                        if (mostnegativeerrorleft > sibling.mostnegativeerrorleft) mostnegativeerrorleft = sibling.mostnegativeerrorleft;
                        if (mostpositiveerrorleft < sibling.mostpositiveerrorleft) mostpositiveerrorleft = sibling.mostpositiveerrorleft;
                        if (mostnegativeerrorleft > sibling.mostnegativeerrorright) mostnegativeerrorleft = sibling.mostnegativeerrorright;
                        if (mostpositiveerrorleft < sibling.mostpositiveerrorright) mostpositiveerrorleft = sibling.mostpositiveerrorright;
                    } else {
                        if (mostnegativeerrorright > sibling.mostnegativeerrorleft) mostnegativeerrorright = sibling.mostnegativeerrorleft;
                        if (mostpositiveerrorright < sibling.mostpositiveerrorleft) mostpositiveerrorright = sibling.mostpositiveerrorleft;
                        if (mostnegativeerrorright > sibling.mostnegativeerrorright) mostnegativeerrorright = sibling.mostnegativeerrorright;
                        if (mostpositiveerrorright < sibling.mostpositiveerrorright) mostpositiveerrorright = sibling.mostpositiveerrorright;
                    }

                    sibling = sibling.previousSibling;
                }

                //cout << "Checked all children!" << endl;


                if ((prevfrontlinenode != null) && (prevfrontlinenode.errorhanging == true)) { // wihtout pointers 16/2/2005
                    //outfile << "Using hanging error at level " << prevfrontlinenode.level << endl;
                    //outfile << "Hanging error's relation to parent is " << prevfrontlinenode.reltoparenterrors << endl;
                    if (prevfrontlinenode.reltoparenterrors == leftRel) {
                        if (mostnegativeerrorleft > prevfrontlinenode.negativeerror) mostnegativeerrorleft = prevfrontlinenode.negativeerror;
                        if (mostpositiveerrorleft < prevfrontlinenode.positiveerror) mostpositiveerrorleft = prevfrontlinenode.positiveerror;
                    } else { // will never happen! Indeed it does not happen! 20/2/2005
                        //outfile << "It HAPPENED!" << endl;
                        if (mostnegativeerrorright > prevfrontlinenode.negativeerror) mostnegativeerrorright = prevfrontlinenode.negativeerror;
                        if (mostpositiveerrorright < prevfrontlinenode.positiveerror) mostpositiveerrorright = prevfrontlinenode.positiveerror;
                    }

                    prevfrontlinenode.errorhanging = false;
                }

                climbnode.mostnegativeerrorleft = mostnegativeerrorleft;
                climbnode.mostpositiveerrorleft = mostpositiveerrorleft;
                climbnode.mostnegativeerrorright = mostnegativeerrorright;
                climbnode.mostpositiveerrorright = mostpositiveerrorright;
            } // finished error calculations


            climbnode.maxabserror = singleerror(climbnode); // added 16/2/2005

            // new climbnode created, now we have to make a heap node for it

            if (topheap.data == null) {
                topheap.data = climbnode;
                climbnode.heap = topheap;
            } else { // normal heap node after topheap, 16/2/2005
                if (freeplace == null) { // there is no free position in the heap to put the new datanode
                    heaptemp = new HeapNode();
                    heaptemp.parent = parentheap;
                    heaptemp.left = null;
                    heaptemp.right = null;
                    heaptemp.next = null;
                    heaptemp.previous = prevheap;
                    prevheap.next = heaptemp;
                    prevheap = heaptemp;

                    if (parentheap.left == null) parentheap.left = heaptemp;
                    else {
                        parentheap.right = heaptemp;
                        parentheap = parentheap.next;
                    }
                } else {
                    heaptemp = freeplace.freeHeapNode;
                    freeplace = freeplace.previousPlace;
                }


                heaptemp.data = climbnode;
                climbnode.heap = heaptemp;

                fixheapup(heaptemp); // re-adjust heap after new climbnode is inserted, 16/2/2005

                //outfile << "Inserted in heap, topheap now points to " << topheap.data.index << " of error " << topheap.data.maxabserror << endl;
                //cout << "Inserted in heap, topheap now points to " << topheap.data.index << " of error " << topheap.data.maxabserror << endl;
            }


            if (frontlinenode == null) { // creating frontlinenode on top of tree structure, new level
                frontlinenode = new PointerNode();
                //frontlinenode.previous = prevfrontlinenode;
                prevfrontlinenode.next = frontlinenode;
                frontlinenode.next = null;
                frontlinenode.hangChild = null; // 14/2/2005
                frontlinenode.level = level;
                frontlinenode.orderinlevel = climbnode.orderinlevel; // alternative assignement also possible

                //if (climbnode.orderinlevel == 1) cout << "Orderinlevel CORRECT = 1" << endl;
                //else cout << "Orderinlevel WRONG" << climbnode.orderinlevel << endl;

                frontlinenode.errorhanging = false; // initialization 16/2/2005

                frontlinenode.storedvalue = climbvalue;
            } else { // creating tree structure, written at 12/2/2005 Normal Case, internal node added
                prevstoredvalue = frontlinenode.storedvalue;
                frontlinenode.storedvalue = climbvalue;

                //if (climbnode.orderinlevel == frontlinenode.orderinlevel + 1) cout << "Orderinlevel CORRECT " << climbnode.orderinlevel << endl;
                //else cout << "Orderinlevel WRONG frontline " << frontlinenode.orderinlevel << " climbnode " << climbnode.orderinlevel << endl;

                frontlinenode.orderinlevel = climbnode.orderinlevel;
            }
            prevfrontlinenode = frontlinenode;

            frontlinenode = frontlinenode.next;

            if ((highest == null) || (highest.level < climbnode.level)) { // it works like this because we have powerof2 data
                highest=climbnode;
                //outfile << "Highestnode new ascended to " << climbnode.index << " of level " << climbnode.level << endl;
                //cout << "Highestnode new ascended to " << climbnode.index << " of level " << climbnode.level << endl;
            }

            // I believe that hanging should be done here instead, appropriately 15/2/2005

            if ((prevfrontlinenode.hangChild == null)) { // ((prevfrontlinenode.errorhanging == 0))
                //if (number % 2 == 0) // null hangchild before top ascending level!
                //{
                //if (prevfrontlinenode.errorhanging == 1) cout << "YES, it happens like this!" << endl;
                //else cout << "NO, it does not happen like this!"<< endl;
                //}

                //if (number % 2 == 0) cout << "No hangchild but mod is 0!" << endl;
                //else cout << "No hangchild and mod is 1!" << endl;
                prevfrontlinenode.hangChild = climbnode; // node climbed to hanged on frontline 15/2/2005
                climbnode.hanged = prevfrontlinenode;
                //outfile << "Hanged hangchild " << climbnode.index << " to be used later" << endl;
            }

            //cout << "Finished this part of climbing loop!" << endl;

        } // climbing loop ends here

        globalaverage = climbvalue; // added 26/2/2005

//outfile << "Finished climbing" << endl;
    }

    DataNode discard_calculate(double data1, double data2/*ofstream& outfile,*/ ) {
//        DataNode singlenode, sibling = null;
        int number, step;
//        double data1, data2;


        if (globalNumber< space) { // go through the first space data
            globalNumber+=2; // add 2, not just 1, since you take two
//            data1 = Double.parseDouble(in.next());
//            data2 = Double.parseDouble(in.next());
//            infile >> data1 >> data2;

            climbup(globalNumber, size +globalNumber-1,data1, data2 /*, outfile*/);
        }


        else { //(number<size) // read all the rest data from the stream in pairs, making space for them step-by-step
            // take in two more data from stream first
//            data1 = Double.parseDouble(in.next());
//            data2 = Double.parseDouble(in.next());
            globalNumber+=2;
//            infile >> data1 >> data2;

            climbup(globalNumber, size +globalNumber-1,data1, data2 /*, outfile*/);

            step=0;
            while (step<2) { // threshold two coefficients from existing view
                singlenode = topheap.data;

                //cout << "Discarding index " << singlenode.index << " of error value " << singlenode.data << endl;
                //outfile << "Discarding index " << singlenode.index << " of data " << singlenode.data << " and max error value " << singlenode.maxabserror << endl;

                if (singlenode.index == 53) {
                    System.out.println();
                }

                //outfile << "Errors on 13 are " << singlenode.mostpositiveerrorleft << " " << singlenode.mostnegativeerrorleft << " " << singlenode.mostpositiveerrorright << " " << singlenode.mostnegativeerrorright << endl;
                propagateerror(singlenode/*, outfile*/);
//                deleteHeap(singlenode.heap);


                sibling = singlenode.leftChild; // 14/2/2005
                if (sibling != null) { //&& (sibling.index < size)) // 14/2/2005
                    //outfile << "Considering leftchild " << sibling.index << " for node " << singlenode.index << endl;

                    if (singlenode.hanged != null) { // check this first, before sibling moves!
                        singlenode.hanged.hangChild = sibling; // 14/2/2005 integration
                        sibling.hanged = singlenode.hanged; // reconnection both ways!
                        //outfile << "Hangchild " << sibling.index << " at the place of " << singlenode.index << endl;
                    }

                    //outfile << "Gave previoussibling ";
                    sibling.previousSibling = singlenode.previousSibling; // 14/2/2005
                    if (singlenode.previousSibling != null) {
                        singlenode.previousSibling.nextSibling = sibling; // 14/2/2005
                        //outfile << singlenode.previoussibling.index;
                    }
                    //else outfile << "null";
                    //outfile << " to node " << sibling.index << endl;

                    while (sibling.nextSibling != null) { // 14/2/2005
                        sibling.parent = singlenode.parent;
                        sibling.reltoparent = singlenode.reltoparent;
                        sibling = sibling.nextSibling;
                    }
                    sibling.parent = singlenode.parent; // 14/2/2005
                    sibling.reltoparent = singlenode.reltoparent; // 14/2/2005

                    //outfile << "Gave nextsibling ";
                    sibling.nextSibling =  singlenode.nextSibling; // 14/2/2005
                    if (singlenode.nextSibling != null) {
                        singlenode.nextSibling.previousSibling = sibling; // 14/2/2005
                        //outfile << singlenode.nextsibling.index << " to node " << sibling.index << endl;
                        //outfile << "Gave previoussibling " << sibling.index << " to node " << singlenode.nextsibling.index << endl;
                    }
                    //else outfile << "null to node " << sibling.index << endl;
                } else { // 14/2/2005
                    //outfile << "Considering null leftchild for node " << singlenode.index << endl;

                    if (singlenode.nextSibling != null) {
                        singlenode.nextSibling.previousSibling = singlenode.previousSibling;
                        //outfile << "Gave previoussibling ";
                        //if (singlenode.previoussibling != null) outfile << singlenode.previoussibling.index; else outfile << "null";
                        //outfile << " to node " << singlenode.nextsibling.index << endl;
                    }

                    if (singlenode.previousSibling != null) {
                        singlenode.previousSibling.nextSibling = singlenode.nextSibling;
                        //outfile << "Gave nextsibling ";
                        //if (singlenode.nextsibling != null) outfile << singlenode.nextsibling.index; else outfile << "null";
                        //outfile << " to node " << singlenode.previoussibling.index << endl;
                    }

                    if (singlenode.hanged != null) { // should go to nextsibling if it exists in the tree!
                        if (singlenode.nextSibling != null) {
                            singlenode.hanged.hangChild = singlenode.nextSibling;
                            singlenode.nextSibling.hanged = singlenode.hanged; // reconnection both ways!
                            //outfile << "Hangchild " << singlenode.nextsibling.index << " at the place of " << singlenode.index << endl;
                        } else { // only if it is already in tree! 15/2/2005
                            singlenode.hanged.hangChild = null;
                            //outfile << "Hangchild null at the place of " << singlenode.index << endl;
                        }
                    }
                }

                if ((singlenode.parent != null) && (singlenode == singlenode.parent.leftChild)) { // refined on 14/2/2005
                    //outfile << "Leftchild of " << singlenode.parent.index << " changed to ";
                    if ((singlenode.leftChild != null)) { // && (singlenode.leftchild.index < size)) // first 14/2/2005, second 15/2/2005
                        singlenode.parent.leftChild = singlenode.leftChild;
                        //outfile << singlenode.leftchild.index << endl;
                    } else if (singlenode.nextSibling != null) {
                        singlenode.parent.leftChild = singlenode.nextSibling;
                        //outfile << singlenode.nextsibling.index << endl;
                    } else {
                        singlenode.parent.leftChild = null;
                        //outfile << "null" << endl;
                    }
                }

                if (singlenode == highest) {
                    highest = null;
                }

                nodecounter--;

                singlenode = null;
                step++;
            } // finished two steps of discarding
        }

        return sibling;
// padding process must be added here 17/2/2005



//        System.out.println("Total nodes " + (nodecounter+1));

//outfile.close();
    }

    void padding(DataNode sibling){
        int poweroftwo = (int)Math.pow(2,(int)(Math.log(globalNumber)/Math.log(2)+.9999999));
//        if(number == poweroftwo){
//            number += 2;
//            poweroftwo = (int)Math.pow(2,(int)(Math.log(number)/Math.log(2)+.9999999));
//        }
        if (globalNumber < poweroftwo) { // fill empty space up
            // local climbing here 17/2/2005

            PointerNode frontlinenode = frontlinefirst;
            DataNode climbnode = null;
            DataNode prevclimbnode = null;
            int justentered = 1;
            double climbvalue = 0;

            while ((frontlinenode != null) && (frontlinenode.errorhanging==false) && (frontlinenode.hangChild== null)) // reach next level with hangchild
                frontlinenode = frontlinenode.next; // errorhanging added 22/2/2005

            while (frontlinenode != null) { // go through all level till the top
                if (justentered == 1) {
                    climbvalue = frontlinenode.storedvalue;
                    justentered = 0;
                    prevclimbnode = frontlinenode.hangChild;
                } else {
                    climbvalue = (climbvalue + frontlinenode.storedvalue)/2;
                    double data = frontlinenode.storedvalue - climbvalue;

                    nodecounter++;

                    climbnode = new DataNode();

                    climbnode.level = frontlinenode.level + 1;
                    if (frontlinenode.next != null) climbnode.orderinlevel = frontlinenode.next.orderinlevel + 1;
                    else climbnode.orderinlevel = 1;

                    //climbnode.value = climbvalue;
                    climbnode.data = data;

                    climbnode.leftChild = frontlinenode.hangChild;
                    climbnode.nextSibling= null;
                    climbnode.parent = null;
                    climbnode.hanged = null;

                    System.out.println("Padding coefficient at level " + climbnode.level + " order " + climbnode.orderinlevel );

                    sibling = climbnode.leftChild; // connect all the children of created node to it

                    if (sibling != null) { // corrected 23/2/2005
                        while(sibling.nextSibling != null) {
                            sibling.parent = climbnode;

                            //outfile << "Gave parent of value " << climbnode.value << " and error " << climbnode.data << " to node of error " << sibling.data << endl;
                            sibling.reltoparent = ancestor(climbnode, sibling);
                            sibling = sibling.nextSibling;
                        }
                        //outfile << "Gave parent of value " << climbnode.value << " and error " << climbnode.data << " to node of error " << sibling.data << endl;
                        sibling.parent = climbnode;
                        sibling.reltoparent = ancestor(climbnode, sibling);

                        sibling.nextSibling = prevclimbnode; // why does this happen after giving parents? 23/2/2005
                        if (prevclimbnode != null) {
                            prevclimbnode.previousSibling = sibling;
                            //outfile << "Gave parent of value " << climbnode.value << " and error " << climbnode.data << " to node of error " << sibling.data << endl;
                            prevclimbnode.parent = climbnode; // added 23/2/2005
                            prevclimbnode.reltoparent = ancestor(climbnode, prevclimbnode);
                        }
                    }

                    if ((highest == null) || (highest.level < climbnode.level)) {
                        System.out.println("Fixed highest" );
                        highest=climbnode;
                    }

                    prevclimbnode = climbnode;
                }

                do frontlinenode = frontlinenode.next; // also error hanging 23/2/2005
                while ((frontlinenode != null) && (frontlinenode.errorhanging==false) && (frontlinenode.hangChild == null)); // reach next level with hangchild
            }

            globalaverage = climbvalue; // added 26/2/2005
        }

        rootnode = highest;
    }

    void writevalue(double value) throws Exception {
        bw.write(String.format("%.5f", value) + " ");
        bw.flush();
//        System.out.println("BBBBBBBBBBB "+String.format("%.5f", value));
        cnt++;
    }

    void writevalues(int levels, double enter, DataNode datatemp) throws Exception {
        double dataleft, dataright;
//int intdata;
        DataNode sibling, leftchild, lastchild;
        int leaves, i, copies, leavesdifference;
        int pastcurrent, presentcurrent, pastleft, pastsib, presentsib, presentlast;

        dataleft = enter + datatemp.data;
        dataright = enter - datatemp.data;

        leftchild = datatemp.leftChild;
//if (leftchild != NULL) cout << "Leftchild of " << datatemp->index << " is " << leftchild->index << endl;

        leaves = descendants(datatemp);
        pastcurrent = past(datatemp);
        presentcurrent = present(datatemp);

//outfile << endl << "Start " << leaves << " Descendants of " << datatemp->index << endl;
//cout << endl << "Start " << leaves << " Descendants of " << datatemp->index << endl;

        if (leftchild != null) { // if node has any children at all
            pastleft = past(leftchild);

            leavesdifference = pastleft - pastcurrent;

            if (leavesdifference > 0) { // if first child leaves behind more than its parent does
                //outfile << "Covering a front gap of " << leavesdifference << " for " << datatemp->index << " with first child " << leftchild->index << endl;

                if (leftchild.reltoparent == rightRel) {
                    for (i=0; i<(leaves/2); i++) writevalue(dataleft);

                    copies = leavesdifference - (leaves/2);
                    for (i=0; i<copies; i++) writevalue(dataright);
                } else {
                    for (i=0; i<leavesdifference; i++) writevalue(dataleft);
                }
            }

            if (leftchild.reltoparent == leftRel) writevalues(levels, dataleft, leftchild);
            else writevalues(levels, dataright, leftchild);


            // everything before the first child printed now, we move on

            lastchild = leftchild;
            sibling = leftchild.nextSibling;

            //if (sibling != NULL) cout << "Sibling of " << leftchild->index << " is " << sibling->index << endl;

            presentlast = present(lastchild);
            while (sibling != null) {
                pastsib = past(sibling);
                presentsib = present(sibling);

                leavesdifference = pastsib - presentlast;

                if (leavesdifference > 0) { // somes leaves between are missing
                    //outfile << "Covering a middle gap of " << leavesdifference << " for " << datatemp->index << endl;
                    //cout << "Covering a middle gap of " << leavesdifference << " for " << datatemp->index << endl;

                    if (sibling.reltoparent == leftRel) { // all missing leaves are on the left side of parent
                        for (i=0; i<leavesdifference; i++) writevalue(dataleft);
                    } else if (sibling.previousSibling.reltoparent == rightRel) { // all missing leaves are on the right side of parent
                        for (i=0; i<leavesdifference; i++) writevalue(dataright);
                    } else { // some missing leaves are left, some right
                        for (i=0; i<(leaves/2) - (present(sibling.previousSibling) - pastcurrent); i++) writevalue(dataleft);
                        for (i=0; i<(pastsib - pastcurrent) - (leaves/2); i++) writevalue(dataright);
                    }
                }

                if (sibling.reltoparent == leftRel) writevalues(levels, dataleft, sibling);
                else writevalues(levels, dataright, sibling);

                lastchild = sibling;
                presentlast = present(lastchild); // this one was missing!
                sibling = sibling.nextSibling;

                //if (sibling != NULL) cout << "Sibling of " << lastchild->index << " is " << sibling->index << endl;
            }

            presentlast = present(lastchild);
            leavesdifference = presentcurrent - presentlast;
            if (leavesdifference > 0) { // last child not last in its level
                //outfile << "Covering an end gap of " << leavesdifference << " for " << datatemp->index << endl;

                if (lastchild.reltoparent == rightRel) {
                    for (i=0; i<leavesdifference; i++) writevalue(dataright);
                } else {
                    copies = leavesdifference -(leaves/2);
                    for (i=0; i<copies; i++) writevalue(dataleft);

                    for (i=0; i<(leaves/2); i++) writevalue(dataright);
                }
            }
        } else { // if this node does not have any children at all
            for (i=0; i<(leaves/2); i++) writevalue(dataleft);
            for (i=0; i<(leaves/2); i++) writevalue(dataright);
        }

//outfile << endl << "End Descendants of " << datatemp->index << endl;
    }


    public static int startIndex(DataNode node){
        if(node == null) return -1;
        return ((node.orderinlevel - 1) * (int)Math.pow(2, node.level)) + 1;
    }

    public static int endIndex(DataNode node){
        if(node == null) return -1;
        return node.orderinlevel * (int)Math.pow(2, node.level);
    }

}
