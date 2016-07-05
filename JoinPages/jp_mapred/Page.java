public class Page {
	private String bd;
	private String title;
	private String url;
	
	Page() {
		bd = "";
		title = "";
		url = "";
	}
	
	Page(String bd, String title, String url) {
		this.bd = bd;
		this.title = title;
		this.url = url;
	}
	
	Page(String info) {        
        String temp = "";
        this.bd = "";
        this.title = "";
        this.url = "";
        
        for(int i = 0, j = 0; i < info.length(); i++){
            if(info.charAt(i) == '$'){
                if(j == 0)
                    this.bd = temp;
                else if(j == 1)
                    this.title = temp;
                else 
                    this.url = temp;
                temp = "";
                j++;
            }else
                temp += info.charAt(i);
        }    
        if(temp.length() != 0)
            this.url = temp;                
    }
	
	public String serializePage() {
		return this.bd + "$" + this.title + "$" + this.url;
	}
	
	public String getTitle() {
		return this.title;
	}
	
	public String getUrl() {
		return this.url;
	}
	
	public String getBD() {
		return this.bd;
	}
}