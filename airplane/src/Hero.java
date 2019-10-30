import java.awt.image.BufferedImage;

public class Hero extends FlyingObject {
    private BufferedImage[] images = {};
    private int index = 0;
    private int doubleFire;
    private int life;
    public Hero(){
        life = 3;
        doubleFire = 0;
        images = new BufferedImage[]{ShootGame.hero0,ShootGame.hero1};
        image = ShootGame.hero0;
        width = image.getWidth();
        height = image.getHeight();
        x = 150;
        y = 400;
    }
    public int isDoubleFire(){
        return doubleFire;
    }
    public void setDoubleFire(int doubleFire){
        this.doubleFire = doubleFire;
    }
    public void addDoubleFire(){
        doubleFire = 40;
    }
    public void addLife(){
        life++;
    }
    public void subtractLife(){
        life--;
    }
    public int getlife(){
        return life;
    }

    public void moveTo(int x,int y){
        this.x = x -width/2;
        this.y = y - height/2;
    }

    @Override
    public boolean outOfBounds() {
        return false;
    }
    public Bullet[] shoot(){
        int xStep = width/4;
        int yStep = 20;
        if(doubleFire>0){
            Bullet[] bullets = new Bullet[2];
            bullet[0] = new Bullet(x+xStep,y-yStep);
            bullets[1] = new Bullet(x+3*xStep,y-yStep);
            return bullets;
        }
    }

    @Override
    public void step() {
        if(image.length>0);{
            image = images[index++/10%images.length]
        }
    }
}
