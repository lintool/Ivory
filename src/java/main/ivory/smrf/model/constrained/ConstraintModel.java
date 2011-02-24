/*
 * Ivory: A Hadoop toolkit for Web-scale information retrieval
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package ivory.smrf.model.constrained;

import ivory.smrf.model.Clique;

import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * @author Lidan Wang
 *
 */
 public class ConstraintModel {

	public static List<ConstrainedClique> greedyJoint (List<ConstrainedClique> cliques, double binConstraint, double unigramAddThreshold, double bigramAddThreshold, double unigramRedundThreshold, double bigramRedundThreshold, double beta){
		Deque<ConstrainedClique> sortedCliques = orderByProfitDensity(cliques);

		List<ConstrainedClique> selectedCliques =  new ArrayList<ConstrainedClique>();

		double totalCost = 0.0;

		Set<String> addedConcepts = new HashSet<String>();

		Deque<ConstrainedClique> sortedCliques2 = new LinkedList<ConstrainedClique>();
		
		while (sortedCliques.size()!=0 || sortedCliques2.size()!=0){
			boolean fromQueue2 = false;
			ConstrainedClique c;
			if (sortedCliques.size()!=0){
				c = sortedCliques.removeFirst();
			}			
			else{
				c = sortedCliques2.removeFirst();
				fromQueue2 = true;
			}

			double cost = c.getAnalyticalCost();
			double newCost = totalCost + cost;

			double conceptWgt = c.getWeight();

			if (((newCost- binConstraint))<binConstraint*0.07 && (c.getWeight() >= 0.00001)){ 

				String concept = c.getConcept();
				Clique.Type conceptType = c.getType();

				boolean addConcept = false;

				//see if passing the redundancy threshold
				if (addedConcepts.contains(concept)){
					if (conceptType.equals(Clique.Type.Term)) {
						if (conceptWgt >  unigramRedundThreshold){
							addConcept = true;
						}
					}
					else{
						if (conceptWgt >  bigramRedundThreshold){
							addConcept = true;
						}
					}

				}
				//see if passing the add threshold
				else{
					if (conceptType.equals(Clique.Type.Term)){
						if (conceptWgt > unigramAddThreshold){
							addConcept = true;
						}
					}
					else{
						if (conceptWgt > bigramAddThreshold) {
							addConcept = true;
						}
					}
				}

				if (!addConcept && !fromQueue2){
					if ((conceptWgt - beta) > 0){
						sortedCliques2.add(c);
					}

				}
				else{
					totalCost = newCost;
					selectedCliques.add(c);

					addedConcepts.add(concept);
				}
			}
		}

		return selectedCliques;
	}



	public static List<ConstrainedClique> greedyKnapsack (List<ConstrainedClique> cliques, double binConstraint, double unigramAddThreshold, double bigramAddThreshold) {

		//System.out.println("Using independent model with add threshold... binConstraint is "+binConstraint);

		Deque<ConstrainedClique> sortedCliques = orderByProfitDensity (cliques);

		List<ConstrainedClique> selectedCliques = new ArrayList<ConstrainedClique>();

		double totalCost = 0.0;

		while (sortedCliques.size()!=0){

			ConstrainedClique c = sortedCliques.removeFirst();
			Clique.Type conceptType = c.getType();
			double conceptWgt = c.getWeight();

			double cost = c.getAnalyticalCost();
			double newCost = totalCost + cost;

			if (((newCost- binConstraint)<binConstraint*0.07) && (conceptWgt  >= 0.00001)){
				boolean addConcept = false;

				if (conceptType.equals(Clique.Type.Term)){
					if (conceptWgt > unigramAddThreshold){
						addConcept = true;
					}
				}
				else{
					if (conceptWgt > bigramAddThreshold){
						addConcept = true;
					}
				}
				if (addConcept) {
					totalCost = newCost;
					selectedCliques.add(c);
				}
			}                      
		}

		return selectedCliques;             
	}


	public static Deque<ConstrainedClique> orderByProfitDensity (List<ConstrainedClique> cliques){

		//hold unique concept terms
		List<String> holder1 = new ArrayList<String>(); 

		//hold profit/weight/profitDensity value of each unique concept term
		List<String> holder2 = new ArrayList<String>();

		HashMap<String, List<ConstrainedClique>> featureOrder = new HashMap<String, List<ConstrainedClique>>();

		for (int i=0; i<cliques.size(); i++){
			ConstrainedClique c = cliques.get(i);
			String term = c.getConcept();
			if (featureOrder.containsKey(term)){
				List<ConstrainedClique> l = featureOrder.get(term);
				l.add(c);
			}
			else{
				List<ConstrainedClique> l = new ArrayList<ConstrainedClique>();
				l.add(c);
				featureOrder.put(term, l);
				holder1.add(term);

				double p = 0 - c.getProfitDensity();

				holder2.add(p+"");
			}
		}

		double [] values = new double[holder2.size()];
		for (int i=0; i<values.length; i++){
			values[i] = Double.parseDouble((String)(holder2.get(i)));
		}               

		List<String> reorderedTerms = orderCliques(holder1, values);

		Deque<ConstrainedClique> sortedCliques = new LinkedList<ConstrainedClique>();

		for (int i=0; i<reorderedTerms.size(); i++){
			String term = reorderedTerms.get(i);
			List<ConstrainedClique> l = featureOrder.get(term);

			for (int j=0; j<l.size(); j++){
				sortedCliques.add(l.get(j));
			}
		}

		return sortedCliques;

	}


	//order cliques by asending order of values[]
	public static List<String> orderCliques (List<String> cliques, double[] values){

		int [] order = new int[values.length];

		for (int i=0; i<order.length; i++){
			order[i] = i;
		}

		Quicksort (values, order, 0, order.length-1);

		List<String> returnCliques = new ArrayList<String>();

		for (int i=0; i<order.length; i++){
			int index = order[i];

			returnCliques.add(cliques.get(index));
		}

		return returnCliques;		
	}



	public static void Quicksort( double vec[], int order [], int loBound, int hiBound )
	//..................................................................
	// PRE: Assigned(loBound) && Assigned(hiBound)
	//      && Assigned(vec[loBound..hiBound])
	// POST: vec[loBound..hiBound] contain same values as
	//      at invocation but are sorted into ascending order
	//..................................................................
	{

		double pivot;

		int loSwap;
		int hiSwap;
		double temp;
		int orderTemp;
		int orderPivot;

		if (loBound >= hiBound) // Zero or one item to sort
		return;
		if (hiBound-loBound == 1) { // Just two items to sort
			if (vec[loBound] > vec[hiBound]) {
				temp = vec[loBound];
				orderTemp=order[loBound];
				vec[loBound] = vec[hiBound];
				order[loBound]=order[hiBound];
				vec[hiBound] = temp;
				order[hiBound]=orderTemp;

			}
			return;
		}
		// 3 or more items to sort
		pivot = vec[(loBound+hiBound)/2]; //use middle as pivot for performance
		orderPivot=order[(loBound+hiBound)/2]; 
		vec[(loBound+hiBound)/2] = vec[loBound];
		order[(loBound+hiBound)/2]=order[loBound];

		vec[loBound] = pivot;
		order[loBound]=orderPivot;

		loSwap = loBound + 1;
		hiSwap = hiBound;

		do { //the partitioning
			while (loSwap <= hiSwap && vec[loSwap] <= pivot){
				loSwap++;
			}
			while (vec[hiSwap] > pivot){
				hiSwap--;
			}
			if (loSwap < hiSwap) {
				temp = vec[loSwap];
				orderTemp=order[loSwap];
				vec[loSwap] = vec[hiSwap];
				vec[hiSwap] = temp;
				order[loSwap]=order[hiSwap];  
				order[hiSwap]=orderTemp;
			}
		} while (loSwap < hiSwap);

		//put pivot back in correct position
		vec[loBound] = vec[hiSwap];
		vec[hiSwap] = pivot;
		order[loBound]=order[hiSwap];
		order[hiSwap]=orderPivot;

		Quicksort(vec, order, loBound, hiSwap-1); 
		Quicksort(vec, order, hiSwap+1, hiBound);
	}


 }
