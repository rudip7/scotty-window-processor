package Synopsis.Wavelets.Old;


public class WaveletSynopsis {

    int number;
    double data1;
    SiblingTree siblingTree;
    DataNode currentSibling;

    public WaveletSynopsis(int size, int space) throws Exception {
        this.number = 0;
        siblingTree = new SiblingTree(size, space);
    }

    public void update(Number in){
        if (number % 2 == 0){
            data1 = in.doubleValue();
        }else {
            currentSibling = siblingTree.discard_calculate(data1, in.doubleValue());
        }
        number++;
    }

    public double PointQuery(int idx){
        DataNode root = siblingTree.rootnode;
        return privatePointQuery(root, idx, SiblingTree.startIndex(root), SiblingTree.endIndex(root), siblingTree.globalaverage);
    }

    private static double privatePointQuery(DataNode dataNode, int idx, int start, int end, double val){
        if(start == end || dataNode == null ){
            return val;
        }
        if(dataNode.leftChild == null){
            int mid = (start + end)/2;
            //            System.out.println(dataNode.data);
            return (idx <= mid) ? val + dataNode.data : val - dataNode.data;
        }
        //        System.out.println(start+" "+end);
        int leftStart = SiblingTree.startIndex(dataNode.leftChild);
        int leftEnd= SiblingTree.endIndex(dataNode.leftChild);

        int rightStart = SiblingTree.startIndex(dataNode.leftChild.nextSibling);
        int rightEnd= SiblingTree.endIndex(dataNode.leftChild.nextSibling);
        if(idx >= leftStart && idx <= leftEnd){
            return privatePointQuery(dataNode.leftChild, idx, leftStart, leftEnd, val + dataNode.data);
        }
        else if(idx >= rightStart && idx <= rightEnd){
            return privatePointQuery(dataNode.leftChild.nextSibling, idx, rightStart, rightEnd, val - dataNode.data);
        }
        else return val;
    }

    public double RangeSumQuery(int leftIndex, int rightIndex){
        DataNode root = siblingTree.rootnode;
        return privateRangeSumQuery(root, leftIndex, rightIndex, (rightIndex-leftIndex+1)*siblingTree.globalaverage);
    }

    private static double privateRangeSumQuery(DataNode dataNode, int l, int h, double sum){

        if(dataNode == null) return sum;
        int mid = (h + l)/2 ; // rightest index of left side leaf

        //        int leftLeaves = mid - l + 1;
        //        int rightLeaves = h - mid;
        //        sum += ((dataNode.data) * (Math.abs(leftLeaves) - Math.abs(rightLeaves)));
        //        System.out.println("diffs: " + (Math.abs(leftLeaves) - Math.abs(rightLeaves)) +" dataNode:"+dataNode.data);
        //        System.out.println( l+" "+h+" sum: "+sum);


        if(dataNode.leftChild == null){
            int st = SiblingTree.startIndex(dataNode);
            int en = SiblingTree.endIndex(dataNode);
//                System.out.println("HITUNG! LEFT CHILD ABIS! " + st +" "+ en);
            int split = (st + en)/2;
            double ans = 0;
//                System.out.println(ans+" "+l +" "+ split+" " + h);
            for(int i = l ; i <= Math.min(split, h) ; i++){ // left
                ans += dataNode.data;
            }
//                System.out.println(ans);
            for(int i = h ; i > Math.max(split, l) ; i--){ // right
                ans -= dataNode.data;
            }
//                System.out.println(ans);
            return ans + sum;
        }

        int leftStart = SiblingTree.startIndex(dataNode.leftChild);
        int leftEnd= SiblingTree.endIndex(dataNode.leftChild);

        int leftmaxstart = Math.max(l, leftStart);
        int leftminend = Math.min(h, leftEnd);

        int rightStart = SiblingTree.startIndex(dataNode.leftChild.nextSibling);
        int rightEnd= SiblingTree.endIndex(dataNode.leftChild.nextSibling);

        int rightmaxstart = Math.max(l, rightStart);
        int rightminend = Math.min(h, rightEnd);

        //        System.out.println(l+" "+ h +" "+leftStart+" "+leftEnd+" "+rightStart+" "+ rightEnd);
        //
        //        System.out.println(leftmaxstart+" "+leftminend+" "+rightmaxstart+" "+ rightminend);

        int left = (leftminend < leftmaxstart) ? 0 : leftminend - leftmaxstart + 1;
        int right = (rightminend < rightmaxstart) ? 0 : rightminend - rightmaxstart + 1;

//            System.out.println("left: " + left +" right:"+ right+" \ndataNode:"+dataNode.data);

        sum += dataNode.data*(Math.abs(left) - Math.abs(right));
//            System.out.println(l +" "+ h + " " +sum);
        //        System.out.println();

        if(l >= leftStart && h <= leftEnd){
            //            if(l + h % 2 == 1)
            return privateRangeSumQuery(dataNode.leftChild, l, h, sum);
            //            else
            //                return privateRangeSumQuery(dataNode.leftChild, l, mid, sum);
        }
        else if(l >= rightStart  && h <= rightEnd ){
            //            if(l + h % 2 == 1)
            //                return privateRangeSumQuery(dataNode.leftChild.nextSibling, mid, h, sum);
            //            else
            return privateRangeSumQuery(dataNode.leftChild.nextSibling, l, h, sum);

        }
        return sum ;
        // query: 1 2
        // cur: 1 8 mid = 4
        // h - mid = 6
        // mid - l + 1 = 4

        // query: 3 8
        // cur: 1 8 mid = 4
        // 8-4 = 4 (h - mid)
        // 4-3+1 = 2 (mid-l+1)

        // call l, mid
        // cur 3 4 mid = 2
        // h - mid >> 4-2=2
        // mid-l+1 >> 2-3+1=0

        // call mid+1, h
        // cur 5 8 mid = 6
        // h - mid >> 8 - 6 = 2
        // mid - l +1 >> 6-5+1 = 2
    }
}
