import { Injectable, signal, effect } from '@angular/core';
import { HttpClient, HttpParams } from '@angular/common/http';
import { Observable, tap } from 'rxjs';

export interface User {
  id: number;
  first_name: string;
  last_name: string;
  email: string;
  phone_number?: string;
  region?: string;
  country?: string;
}

@Injectable({
  providedIn: 'root'
})
export class ApiService {
  private readonly baseUrl = 'http://localhost:5001/api';
  
  // State management using Angular Signals
  currentUser = signal<User | null>(null);
  visitorId = signal<string>('');
  cartCount = signal<number>(0);

  constructor(private readonly http: HttpClient) {
    this.loadSession();
    
    // Automatically refresh cart count when user logs in or out
    effect(() => {
      if (this.currentUser()) {
        this.refreshCartCount();
      } else {
        this.cartCount.set(0);
      }
    });
  }

  private loadSession() {
    // Try to load logged in user
    const savedUser = localStorage.getItem('mlops_user');
    if (savedUser) {
      try {
        this.currentUser.set(JSON.parse(savedUser));
      } catch {
        localStorage.removeItem('mlops_user');
      }
    }

    // Try to load or generate visitor ID for anonymous tracking
    let vid = localStorage.getItem('mlops_visitor_id');
    if (!vid) {
      vid = 'visitor_' + Math.random().toString(36).substring(2, 11);
      localStorage.setItem('mlops_visitor_id', vid);
    }
    this.visitorId.set(vid);
  }

  refreshCartCount() {
    const userId = this.currentUser()?.id;
    if (userId) {
      this.http.get<any[]>(`${this.baseUrl}/cart`, {
        params: new HttpParams().set('user_id', userId.toString())
      }).subscribe({
        next: (items) => {
          const count = items.reduce((acc, item) => acc + item.quantity, 0);
          this.cartCount.set(count);
        },
        error: () => this.cartCount.set(0)
      });
    } else {
      this.cartCount.set(0);
    }
  }

  // Auth Methods
  register(userData: any): Observable<any> {
    return this.http.post(`${this.baseUrl}/register`, userData);
  }

  login(credentials: any): Observable<any> {
    return this.http.post<any>(`${this.baseUrl}/login`, credentials).pipe(
      tap(res => {
        if (res.user) {
          this.currentUser.set(res.user);
          localStorage.setItem('mlops_user', JSON.stringify(res.user));
        }
      })
    );
  }

  logout() {
    this.currentUser.set(null);
    localStorage.removeItem('mlops_user');
    this.cartCount.set(0);
  }

  // Category Methods
  getCategories(): Observable<any[]> {
    return this.http.get<any[]>(`${this.baseUrl}/categories`);
  }

  // Product Methods
  getProducts(page: number = 1, limit: number = 20, search: string = ''): Observable<any> {
    let params = new HttpParams()
      .set('page', page.toString())
      .set('limit', limit.toString());
    
    if (search) {
      params = params.set('search', search);
    }
    
    return this.http.get<any>(`${this.baseUrl}/products`, { params });
  }

  getProduct(id: number, isRecommended: boolean = false): Observable<any> {
    let params = new HttpParams().set('is_recommended', isRecommended ? 'true' : 'false');
    
    if (this.currentUser()) {
      params = params.set('user_id', this.currentUser()!.id.toString());
    } else {
      params = params.set('visitor_id', this.visitorId());
    }

    return this.http.get<any>(`${this.baseUrl}/products/${id}`, { params });
  }

  getProductsByCategory(categoryId: number, page: number = 1, limit: number = 20): Observable<any> {
    const params = new HttpParams()
      .set('page', page.toString())
      .set('limit', limit.toString());

    return this.http.get<any>(`${this.baseUrl}/products/category/${categoryId}`, { params });
  }

  // Cart Methods
  getCart(): Observable<any[]> {
    const userId = this.currentUser()?.id;
    if (!userId) {
      throw new Error('User is not logged in');
    }
    return this.http.get<any[]>(`${this.baseUrl}/cart`, {
      params: new HttpParams().set('user_id', userId.toString())
    }).pipe(
      tap(items => {
        const count = items.reduce((acc, item) => acc + item.quantity, 0);
        this.cartCount.set(count);
      })
    );
  }

  addToCart(productId: number, quantity: number = 1, isRecommended: boolean = false): Observable<any> {
    const userId = this.currentUser()?.id;
    if (!userId) {
      throw new Error('User is not logged in');
    }
    return this.http.post(`${this.baseUrl}/cart/add`, {
      user_id: userId,
      product_id: productId,
      quantity,
      is_recommended: isRecommended
    }).pipe(
      tap(() => this.refreshCartCount())
    );
  }

  removeFromCart(productId: number, quantity?: number): Observable<any> {
    const userId = this.currentUser()?.id;
    if (!userId) {
      throw new Error('User is not logged in');
    }
    return this.http.post(`${this.baseUrl}/cart/remove`, {
      user_id: userId,
      product_id: productId,
      quantity
    }).pipe(
      tap(() => this.refreshCartCount())
    );
  }

  checkout(): Observable<any> {
    const userId = this.currentUser()?.id;
    if (!userId) {
      throw new Error('User is not logged in');
    }
    return this.http.post(`${this.baseUrl}/transaction`, {
      user_id: userId
    }).pipe(
      tap(() => this.cartCount.set(0))
    );
  }

  // Recommendation Engine Method
  getRecommendations(limit: number = 8): Observable<any[]> {
    let params = new HttpParams().set('limit', limit.toString());
    
    if (this.currentUser()) {
      params = params.set('user_id', this.currentUser()!.id.toString());
    } else {
      params = params.set('visitor_id', this.visitorId());
    }

    return this.http.get<any[]>(`${this.baseUrl}/recommendations`, { params });
  }

  // Trigger MLOps training
  trainModel(): Observable<any> {
    return this.http.post(`${this.baseUrl}/train`, {});
  }

  // Trigger User simulation
  startSimulation(): Observable<any> {
    return this.http.post(`${this.baseUrl}/simulate`, {});
  }

  // Get simulation status
  getSimulationStatus(): Observable<any> {
    return this.http.get(`${this.baseUrl}/simulate/status`);
  }
}
