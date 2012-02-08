package no.sw.crp;

import java.util.ArrayList;
import java.util.Map;

public class Main {
    public static final void main(String[] args) throws Exception {
        MapReduce mr = new MapReduce(args[0], args[1]);

        // TODO(bakksjo): Replace this, read from file (given by args[2]) instead.
        ArrayList<String> values = new ArrayList<String>();
        values.add("revailed, and all the chief Korah, and they had made our hand of you to his clothes. And God formed man his hand:) That be bereaved of his house, and said, Because there shall my servant my son mourning. And it not. And thou hast done unto the wilderness: and, behold, I sent me that thing that God took sheep lying words. And he begat Lamech lived after them, That my master made thee a new wine. Let there hath also the dust of Basemath, Esau's wife. These are dead out of Aram. And Abram was concubine to pass on");
        values.add("befall him. And Abimelech rose up a strong ass, and of peoples, and to be the ring, and upon the land, and they said unto me, and they sat upon his daughter-in-law. And Jehovah made a window, and the prostitute, that the he-goats which I buried in the least of Egypt, and his chariot, and Rebekah is the birds of Canaan unto his face: and, behold, Esau thy seed. And he said unto the rams of the commandment of Jacob took him in one was hardened, and Letushim, and saw the river seven ewe lambs of the servant my dead");
        values.add("thy father, and our daughters. And he said, Let us build us with my strength and Serug two years old man, dwelling was the field, and he said, I will give thee yesternight. And the name of them as thou bless thee unto thee. And nations of Israel, that place of water. And the reed-grass. And, behold, it was called his father. And the men are the Plain of Isaac, Go forth the people go up to pass, when he said unto them, Go therefore shall no plant them into the land of the dove found her; And the seventh");
        values.add("arose a Canaanitish woman. And God created and she said, My spirit of thy voice: and moreover I go through them; and called his sons' wives upon beast, and for Moses said unto Laban, and drink, and Aaron. And Jehovah appeared to his blood of the first is for bread, till thou shalt go from off bearing. And he had commanded: and thou mayest freely eat: but all the city, and Kenaz. And it was gathered together, and rest for it be much as Jehovah brought out of Abraham. And the Canaanites, among the ring, and Judah, saying, The sons");
        values.add("feeding the families of his garment in the ark went down by day unto him. And they heard that come, and the eldest, and let Pharaoh that the south. And Jehovah our asses. And Jehovah left hand of the pledge from Paddan, Rachel envied her not, Abram: I know good ears: and Hazarmaveth, and fifty righteous with his wife's name Jehovah said to do in the Jebusite, and said unto him, and with God: if thou hast changed towards the ark; they, neither shall be Abram in their dough which Abraham begat sons of it was purchased from his two");
        mr.run(values);

        Thread.sleep(5000); // Wait until processing is finished.

        // Print results
        for (Map.Entry<String, ArrayList<String>> entry : mr.reducer.output.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }
    }
}
